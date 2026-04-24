"""
adaptive_client.py
Adaptive Planning API client (v43).
Handles discovery, metadata export, and data export.
"""

import csv
import datetime
import io
import json
import logging
import re
import xml.etree.ElementTree as ET

import requests

from config import (
    ADAPTIVE_LOGIN, ADAPTIVE_PASSWORD, ADAPTIVE_BASE_URL,
    DATE_START, DATE_END, EXCLUDE_VERSIONS, EXCLUDE_SHEETS,
    ALWAYS_INCLUDE_VERSIONS, VERSION_LOOKBACK_YEARS,
)

log = logging.getLogger(__name__)
HEADERS = {"Content-Type": "text/xml; charset=UTF-8"}


# -- Helpers ----------------------------------------------------------

def xe(s):
    return (str(s).replace("&", "&amp;").replace('"', "&quot;")
                  .replace("<", "&lt;").replace(">", "&gt;"))


def call_api(xml_payload, timeout=600):
    resp = requests.post(
        ADAPTIVE_BASE_URL,
        data=xml_payload.encode("utf-8"),
        headers=HEADERS,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.text


def parse_cdata(xml_text):
    if 'success="false"' in xml_text:
        m = re.search(r"<message[^>]*>(.*?)</message>", xml_text, re.DOTALL)
        return [], m.group(1).strip() if m else "Unknown API error"
    m = re.search(r"<!\[CDATA\[(.*?)\]\]>", xml_text, re.DOTALL)
    if not m:
        return [], "No CDATA in response"
    csv_text = m.group(1).strip()
    if not csv_text:
        return [], None
    return list(csv.DictReader(io.StringIO(csv_text))), None


def _ga(attrs_str, name):
    mm = re.search(r'\b' + name + r'="([^"]*)"', attrs_str)
    return mm.group(1).replace("&amp;", "&").replace("&quot;", '"') if mm else ""


def _creds():
    return f'<credentials login="{xe(ADAPTIVE_LOGIN)}" password="{xe(ADAPTIVE_PASSWORD)}"/>'


# -- Phase 0: Discovery -----------------------------------------------

def discover():
    return discover_versions(), discover_sheets()


# Folders whose contents should always be excluded
EXCLUDE_VERSION_FOLDERS = [
    "Ancient History",
    "Amanda McKay",
    "Headcount Sync",
]


def discover_versions():
    current_year = datetime.datetime.now().year
    recent_years = set(str(y) for y in range(current_year - VERSION_LOOKBACK_YEARS, current_year + 2))

    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportVersions\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=30)

    # Parse folder hierarchy to exclude versions inside excluded folders
    versions = []
    excluded_folder_depth = None
    current_depth = 0

    for m in re.finditer(r'<(/?)version\b([^>]*?)(/?)>', raw):
        is_close   = m.group(1) == "/"
        attrs_str  = m.group(2)
        self_close = m.group(3) == "/"

        if is_close:
            current_depth -= 1
            if excluded_folder_depth is not None and current_depth < excluded_folder_depth:
                excluded_folder_depth = None
            continue

        vtype = _ga(attrs_str, "type")
        name  = _ga(attrs_str, "name")

        if not self_close:
            current_depth += 1

        # Check if we just entered an excluded folder
        if vtype == "VERSION_FOLDER":
            if name in EXCLUDE_VERSION_FOLDERS:
                excluded_folder_depth = current_depth
            continue

        # Skip versions inside excluded folders
        if excluded_folder_depth is not None:
            continue

        if not name or name in EXCLUDE_VERSIONS:
            continue

        # Always include explicitly listed versions
        if name in ALWAYS_INCLUDE_VERSIONS:
            versions.append(name)
            continue

        # Include versions containing a recent year in their name
        if any(yr in name for yr in recent_years):
            versions.append(name)
            continue

    log.info(f"Discovered {len(versions)} versions: {versions}")
    return versions


def discover_sheets():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportSheets\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=30)
    sheets = {}
    for tag, stype in [("standard-sheet", "standard"), ("cube-sheet", "cube"), ("modeled-sheet", "modeled")]:
        for name in re.findall(rf'<{tag}\s[^>]*name="([^"]*)"', raw):
            name = name.replace("&amp;", "&").replace("&quot;", '"')
            if name in EXCLUDE_SHEETS:
                log.info(f"  Skipping excluded sheet: {name}")
                continue
            sheets[name] = stype
    standard_cube = sum(1 for t in sheets.values() if t in ("standard", "cube"))
    modeled       = sum(1 for t in sheets.values() if t == "modeled")
    log.info(f"Discovered {len(sheets)} sheets: {standard_cube} standard/cube, {modeled} modeled")
    return sheets


# -- Phase 1: Metadata ------------------------------------------------

def export_accounts():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportAccounts\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=60)

    gl, metric, custom, assumptions, attrs = [], [], [], [], []
    stack = []

    for m in re.finditer(r'<(/?)account\b([^>]*?)(/?)>', raw):
        is_close, attrs_str, self_close = m.group(1) == "/", m.group(2), m.group(3) == "/"
        if is_close:
            if stack: stack.pop()
            continue
        acc_id      = _ga(attrs_str, "id")
        acc_code    = _ga(attrs_str, "code")
        acc_name    = _ga(attrs_str, "name")
        acc_type    = (_ga(attrs_str, "type") or _ga(attrs_str, "accountType")).upper()
        is_assump   = _ga(attrs_str, "isAssumption").lower() == "true"
        parent_id   = stack[-1][0] if stack else ""
        parent_name = stack[-1][1] if stack else ""
        parent_code = stack[-1][2] if stack else ""
        row = {"account_id": acc_id, "account_code": acc_code, "account_name": acc_name,
               "account_type": acc_type, "parent_id": parent_id,
               "parent_name": parent_name, "parent_code": parent_code}
        if is_assump or acc_type == "ASSUMPTION":
            assumptions.append(row)
        elif acc_type in ("METRIC", "METRICS"):
            metric.append(row)
        elif acc_type == "CUSTOM":
            custom.append(row)
        else:
            gl.append(row)
        if not self_close:
            stack.append((acc_id, acc_name, acc_code))

    for lm in re.finditer(r'<account\b([^>]*?)/?>', raw):
        id_m = re.search(r'\bid="(\d+)"', lm.group(1))
        if not id_m: continue
        acc_id   = id_m.group(1)
        acc_name = _ga(lm.group(1), "name")
        acc_code = _ga(lm.group(1), "code")
        pos   = lm.end()
        chunk = raw[pos:pos+2000]
        ab = re.search(r'<attributes>(.*?)</attributes>', chunk, re.DOTALL)
        if not ab: continue
        nap = re.search(r'</?account\b', chunk)
        if nap and nap.start() < ab.start(): continue
        for am in re.finditer(r'<attribute\b[^>]+>', ab.group(1)):
            a = am.group(0)
            attr_name  = _ga(a, "name")
            attr_value = _ga(a, "value")
            if attr_name:
                attrs.append({"account_id": acc_id, "account_name": acc_name,
                               "account_code": acc_code, "attribute_name": attr_name,
                               "attribute_value": attr_value})

    log.info(f"Accounts: {len(gl)} GL, {len(metric)} metric, {len(custom)} custom, "
             f"{len(assumptions)} assumptions, {len(attrs)} attributes")
    return gl, metric, custom, assumptions, attrs


def export_levels():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportLevels\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=60)

    levels, level_attrs = [], []
    stack = []

    for m in re.finditer(r'<(/?)level\b([^>]*?)(/?)>', raw):
        is_close, attrs_str, self_close = m.group(1) == "/", m.group(2), m.group(3) == "/"
        if is_close:
            if stack: stack.pop()
            continue
        level_id    = _ga(attrs_str, "id")
        level_name  = _ga(attrs_str, "name")
        short_name  = _ga(attrs_str, "shortName") or _ga(attrs_str, "code")
        parent_id   = stack[-1][0] if stack else ""
        parent_name = stack[-1][1] if stack else ""
        levels.append({"level_id": level_id, "level_name": level_name, "short_name": short_name,
                        "parent_id": parent_id, "parent_name": parent_name})
        if not self_close:
            stack.append((level_id, level_name))

    level_ids = {r["level_id"] for r in levels}
    for lm in re.finditer(r'<level\b([^>]*?)/?>', raw):
        id_m = re.search(r'\bid="(\d+)"', lm.group(1))
        if not id_m or id_m.group(1) not in level_ids: continue
        level_id   = id_m.group(1)
        level_name = _ga(lm.group(1), "name")
        pos   = lm.end()
        chunk = raw[pos:pos+3000]
        ab = re.search(r'<attributes>(.*?)</attributes>', chunk, re.DOTALL)
        if not ab: continue
        nlp = re.search(r'</?level\b', chunk)
        if nlp and nlp.start() < ab.start(): continue
        for am in re.finditer(r'<attribute\b[^>]+>', ab.group(1)):
            a = am.group(0)
            attr_name  = _ga(a, "name")
            attr_value = _ga(a, "value")
            if attr_name:
                level_attrs.append({"level_id": level_id, "level_name": level_name,
                                     "attribute_name": attr_name, "attribute_value": attr_value})

    log.info(f"Levels: {len(levels)} levels, {len(level_attrs)} attributes")
    return levels, level_attrs


def export_dimensions():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportDimensions\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=60)
    dims, dim_attrs = [], []
    try:
        root = ET.fromstring(raw)
        for dim_el in root.findall(".//dimension"):
            dim_name = dim_el.get("name", "").replace("&amp;", "&")
            dim_id   = dim_el.get("id", "")
            for val_el in dim_el.findall(".//value"):
                val_name   = val_el.get("name", "").replace("&amp;", "&")
                val_id     = val_el.get("id", "")
                dims.append({"dimension_id": dim_id, "dimension_name": dim_name,
                              "value_id": val_id, "value_name": val_name,
                              "short_name": val_el.get("shortName", ""),
                              "is_default": val_el.get("isDefault", "")})
                for attr_el in val_el.findall(".//attribute"):
                    attr_name  = attr_el.get("name", "")
                    attr_value = attr_el.get("value", "")
                    if attr_name:
                        dim_attrs.append({"dimension_id": dim_id, "dimension_name": dim_name,
                                          "value_id": val_id, "value_name": val_name,
                                          "attribute_name": attr_name, "attribute_value": attr_value})
    except ET.ParseError as e:
        log.warning(f"Dimensions XML parse error: {e}")
    log.info(f"Dimensions: {len(dims)} values, {len(dim_attrs)} attributes")
    return dims, dim_attrs


def export_versions_meta():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportVersions\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=30)
    rows = []
    for m in re.finditer(r'<version\b([^>]+)/?>', raw):
        attrs = m.group(1)
        vtype = _ga(attrs, "type")
        name  = _ga(attrs, "name")
        if vtype == "VERSION_FOLDER" or not name: continue
        rows.append({"version_id": _ga(attrs, "id"), "version_name": name,
                     "version_type": vtype, "start_plan": _ga(attrs, "startPlan"),
                     "end_ver": _ga(attrs, "endVer"), "is_locked": _ga(attrs, "isLocked"),
                     "currency": _ga(attrs, "currency")})
    log.info(f"Versions: {len(rows)}")
    return rows


def export_time():
    xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<call method=\"exportTime\" callerName=\"adaptive_export\">"
        f"{_creds()}"
        "</call>"
    )
    raw = call_api(xml, timeout=30)
    rows = []
    for m in re.finditer(r'<period\b([^>]+)/?>', raw):
        attrs = m.group(1)
        code  = _ga(attrs, "code")
        if not code: continue
        parts = code.split("/")
        month_num = parts[0] if len(parts) == 2 else ""
        year      = parts[1] if len(parts) == 2 else ""
        rows.append({"period_id": _ga(attrs, "id"), "period_code": code,
                     "period_name": _ga(attrs, "name"), "year": year,
                     "quarter": _ga(attrs, "quarter"), "month_num": month_num,
                     "fiscal_year": _ga(attrs, "fiscalYear")})
    log.info(f"Time periods: {len(rows)}")
    return rows


# -- Phase 2: Standard + Cube data ------------------------------------

# GL root account codes — covers all standard + cube accounts
GL_ROOT_CODES = [
    "Assets", "Liabilities_Equities", "Net_Income", "Income",
    "Other_Income", "Cost_Of_Goods_Sold", "Expenses",
    "Other_Expenses", "ExchangeRate",
]


def export_all_data(version_name):
    log.info(f"  exportData: version={version_name} date={DATE_START} to {DATE_END}")

    sm, sy = _date_parts(DATE_START)
    em, ey = _date_parts(DATE_END)
    chunks = [(_date_str(sm if yr == sy else 1, yr),
               _date_str(em if yr == ey else 12, yr))
              for yr in range(sy, ey + 1)]

    # Build account filter XML using root GL codes
    acct_xml = "".join(
        f'<account code="{xe(c)}" isAssumption="false" includeDescendants="true"/>' 
        for c in GL_ROOT_CODES
    )

    all_rows = []
    seen = set()

    for ds, de in chunks:
        xml = (
            "<?xml version='1.0' encoding='UTF-8'?>"
            "<call method=\"exportData\" callerName=\"adaptive_export\">"
            f"{_creds()}"
            f"<version name=\"{xe(version_name)}\"/>"
            "<format useInternalCodes=\"true\" includeUnmappedItems=\"true\"/>"
            "<filters>"
            f"<accounts>{acct_xml}</accounts>"
            f"<timeSpan start=\"{xe(ds)}\" end=\"{xe(de)}\"/>"
            "</filters>"
            "<rules includeZeroRows=\"false\" timeRollups=\"false\"/>"
            "</call>"
        )
        rows, error = parse_cdata(call_api(xml))
        if error:
            return all_rows, error
        for row in rows:
            key = (row.get("Account Code", ""), row.get("Level Name", ""),
                   row.get("Time", row.get("Period", "")))
            if key not in seen:
                seen.add(key)
                all_rows.append(row)

    log.info(f"  exportData: {len(all_rows)} rows for version={version_name}")
    return all_rows, None


# -- Phase 3: Modeled sheet data --------------------------------------

def export_modeled_sheet(sheet_name, version_name):
    def try_export(is_global):
        xml = (
            "<?xml version='1.0' encoding='UTF-8'?>"
            "<call method=\"exportConfigurableModelData\" callerName=\"adaptive_export\">"
            f"{_creds()}"
            f"<version name=\"{xe(version_name)}\"/>"
            f"<modeled-sheet name=\"{xe(sheet_name)}\" isGlobal=\"{is_global}\" "
            "includeAllColumns=\"true\" isGetAllRows=\"true\" useNumericIDs=\"false\"/>"
            "<filters>"
            f"<timeSpan start=\"{xe(DATE_START)}\" end=\"{xe(DATE_END)}\"/>"
            "</filters>"
            "</call>"
        )
        return parse_cdata(call_api(xml))

    rows, error = try_export("false")
    if error:
        rows2, error2 = try_export("true")
        if not error2:
            return rows2, None
    return rows, error


# -- Date helpers -----------------------------------------------------

def _date_parts(s):
    parts = s.strip().split("/")
    return int(parts[0]), int(parts[1])


def _date_str(m, y):
    return f"{m:02d}/{y}"
