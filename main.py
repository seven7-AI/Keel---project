import csv
import json
import xml.etree.ElementTree as ET
import os
import re
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import pymysql
from pony.orm import Database, PrimaryKey

# =====================================================
# 1) DATABASE CONFIGURATION AND CONNECTION TEST
# =====================================================

DB_HOST = 'europa.ashley.work'
DB_USER = 'student_bi95au'
DB_PASSWORD = 'iE93F2@8EhM@1zhD&u9M@K'
DB_NAME = 'student_bi95au'

# Data file paths
DATA_DIR = 'data'
CUSTOMERS_XML = os.path.join(DATA_DIR, 'customer_data.xml')
POLICIES_JSON = os.path.join(DATA_DIR, 'insurance_policy_data.json')
VEHICLES_CSV = os.path.join(DATA_DIR, 'vehicle_data.csv')
EXTRAS_TXT = os.path.join(DATA_DIR, 'flagged_data.txt')

db = Database()

def test_connection() -> bool:
    try:
        connection = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        connection.close()
        print("✅ Database connection successful.")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

# =====================================================
# 2) TABLE DISCOVERY & MAPPING HELPERS
# =====================================================

def _find_customer_table() -> str:
    with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
    if "CARINSUR_CUSTOMER" in tables:
        return "CARINSUR_CUSTOMER"
    for t in tables:
        if "customer" in t.lower() and t.lower() not in {"carinsur_policy", "carinsur_vehicle"}:
            return t
    raise RuntimeError(f"No customer table found. Existing tables: {tables}")

def _get_table_columns(table_name: str) -> List[Dict[str, Optional[str]]]:
    with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
            rows = cur.fetchall()
            return [{"Field": r[0], "Type": r[1], "Null": r[2], "Key": r[3], "Default": r[4], "Extra": r[5]} for r in rows]

def _pick_column(cols_set: set, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols_set:
            return c
    return None

def _is_numeric(mysql_type: str) -> bool:
    t = mysql_type.lower()
    return any(x in t for x in ["tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "float", "double", "numeric"])

def _required_cols(cols: List[Dict[str, Optional[str]]]) -> set:
    req = set()
    for c in cols:
        if c["Null"] == "NO" and c["Default"] is None and "auto_increment" not in (c["Extra"] or ""):
            req.add(c["Field"])
    return req

def _primary_key(cols: List[Dict[str, Optional[str]]]) -> Tuple[Optional[str], bool]:
    for c in cols:
        if c["Key"] == "PRI":
            return c["Field"], (_is_numeric(c["Type"]))
    return None, False

def _mysql_integer_range(mysql_type: str) -> Tuple[int, int, bool]:
    t = (mysql_type or "").lower()
    base = t.split("(")[0].strip()
    unsigned = "unsigned" in t
    ranges = {
        "tinyint":  (0 if unsigned else -128, 255 if unsigned else 127),
        "smallint": (0 if unsigned else -32768, 65535 if unsigned else 32767),
        "mediumint":(0 if unsigned else -8388608, 16777215 if unsigned else 8388607),
        "int":      (0 if unsigned else -2147483648, 4294967295 if unsigned else 2147483647),
        "integer":  (0 if unsigned else -2147483648, 4294967295 if unsigned else 2147483647),
        "bigint":   (0 if unsigned else -9223372036854775808, 18446744073709551615 if unsigned else 9223372036854775807),
    }
    if base in ranges:
        mn, mx = ranges[base]
        return mn, mx, unsigned
    return (1, 2147483647, False)

CUSTOMER_TABLE_NAME = _find_customer_table()
CUSTOMER_COLS = _get_table_columns(CUSTOMER_TABLE_NAME)
PK_COL, PK_IS_NUMERIC = _primary_key(CUSTOMER_COLS)
if not PK_COL:
    raise RuntimeError(f"Customer table `{CUSTOMER_TABLE_NAME}` has no primary key.")

_PK_META = next(c for c in CUSTOMER_COLS if c["Field"] == PK_COL)
_PK_TYPE = _PK_META["Type"] or ""
_PK_EXTRA = _PK_META["Extra"] or ""
_PK_AUTO = "auto_increment" in _PK_EXTRA.lower()
_PK_MIN, _PK_MAX, _PK_UNSIGNED = _mysql_integer_range(_PK_TYPE)

class Customer(db.Entity):
    _table_ = CUSTOMER_TABLE_NAME
    if PK_IS_NUMERIC:
        id = PrimaryKey(int, column=PK_COL)
    else:
        id = PrimaryKey(str, column=PK_COL)

# =====================================================
# 3) GENERIC PARSERS & UTILITIES
# =====================================================

_PUNCT_RE = re.compile(r"[^a-zA-Z0-9\s]+")

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def _norm_name(s: Optional[str]) -> str:
    s = (s or "").lower().strip()
    s = _PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def _title_safe(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    return " ".join(w.capitalize() for w in str(s).split())

def _split_full_name(full: str) -> Tuple[str, str]:
    full = _norm_name(full)
    if not full:
        return "", ""
    parts = full.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]

def _name_key(first: str, last: str, postcode: Optional[str]) -> Tuple[str, str, str]:
    return (_norm_name(first), _norm_name(last), (_norm(postcode) or "").upper())

def _full_key(first: str, last: str) -> str:
    return f"{_norm_name(first)} {_norm_name(last)}".strip()

def _keynorm(s: str) -> str:
    return re.sub(r"[ _-]+", "", s.strip().lower())

def _get(row: Dict[str, Any], *names: str) -> Optional[str]:
    if not row:
        return None
    if "___normmap" not in row:
        normmap = {}
        for k, v in row.items():
            normmap[_keynorm(k)] = v
        row["_\\__normmap"] = normmap
    normmap = row["_\\__normmap"]
    for n in names:
        val = normmap.get(_keynorm(n))
        if val is not None:
            return val
    return None

def _deterministic_id_within_range(first_name: str, last_name: str, postcode: str) -> int:
    key = f"{_norm_name(first_name)}|{_norm_name(last_name)}|{(postcode or '').upper()}"
    h = int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big", signed=False)
    min_allowed = 1 if _PK_UNSIGNED or _PK_MIN < 0 else max(1, _PK_MIN)
    max_allowed = _PK_MAX if _PK_MAX >= min_allowed else min_allowed + 1000
    span = max(1, (max_allowed - min_allowed + 1))
    return int((h % span) + min_allowed)

def _parse_currency_to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    s = s.replace("£", "").replace("$", "").replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None

def _parse_date_any(s: str) -> datetime:
    s = (s or "").strip()
    fmts = ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y")
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {s}")

def _read_lines_any_encoding(path: str) -> List[str]:
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                return [line.rstrip("\n") for line in f]
        except UnicodeDecodeError:
            continue
    with open(path, "rb") as f:
        return f.read().decode("latin-1", errors="replace").splitlines()

# =====================================================
# 4) EXTRACTION FUNCTIONS
# =====================================================

def read_customers_xml(path: str) -> List[Dict]:
    """
    <users><user firstName="..." lastName="..." salary="..." address_postcode="..." marital_status="..." ... /></users>
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing customers XML at: {path}")

    tree = ET.parse(path)
    root = tree.getroot()

    records: List[Dict] = []
    for user in root.findall(".//user"):
        first = _norm_name(user.attrib.get("firstName", ""))
        last = _norm_name(user.attrib.get("lastName", ""))
        postcode = user.attrib.get("address_postcode", "")
        marital_status = user.attrib.get("marital_status", "") or None
        salary = _parse_currency_to_float(user.attrib.get("salary"))
        address = postcode or None

        if not first or not last:
            continue

        cid = _deterministic_id_within_range(first, last, postcode or "UNKNOWN")
        records.append({
            "id": cid,
            "first_name": first,
            "last_name": last,
            "marital_status": marital_status,
            "salary": salary,
            "address": address,
            "address_postcode": postcode
        })

    print(f"   📊 Extracted {len(records)} customers from XML")
    return records

def read_policies_json(path: str) -> List[Dict]:
    """
    Accepts list-of-dicts OR nested list [[{...}]] like your sample.
    Uses firstName/lastName/postcode + dates/amounts.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing policies JSON at: {path}")

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        rows = json.load(f)

    if isinstance(rows, list) and rows and isinstance(rows[0], list):
        rows = rows[0]

    policies: List[Dict] = []
    for r in rows:
        first = _norm_name(r.get("firstName", ""))
        last = _norm_name(r.get("lastName", ""))
        postcode = r.get("address_postcode", "")

        if not first or not last:
            continue

        try:
            start = _parse_date_any(r.get("insurance_start_date", ""))
            end = _parse_date_any(r.get("insurance_end_date", ""))
        except Exception as e:
            print(f"⚠  Skipping policy due to bad dates for {_full_key(first,last)}: {e}")
            continue

        monthly = _parse_currency_to_float(r.get("monthly_payment_amount"))
        policies.append({
            "customer_lookup": (first, last, (postcode or "").upper()),
            "start_date": start,
            "end_date": end,
            "monthly_payment": monthly,
            "payment_frequency": _norm(r.get("payment_frequency"))
        })

    print(f"   📊 Extracted {len(policies)} policies from JSON")
    return policies

def _extract_vehicle_name(row: Dict[str, str]) -> Tuple[str, str]:
    """Extract first/last name from many possible CSV columns. Handles 'First Name' + 'Second Name'."""
    first = _get(row,
        "first name","firstname","first_name","customer first name","given_name","given name","forename"
    )
    last  = _get(row,
        "last name","lastname","last_name","second name","surname","family_name","family name"
    )
    if first or last:
        return _norm_name(first), _norm_name(last)

    full = _get(row, "customer_name","customer name","name","full_name","full name")
    if full:
        return _split_full_name(full)

    f2 = _get(row, "customer first name","customerfirstname")
    l2 = _get(row, "customer last name","customerlastname")
    return _norm_name(f2 or ""), _norm_name(l2 or "")

def read_vehicles_csv(path: str) -> List[Dict]:
    """
    Flexible columns for model/year and customer names.
    Works with headers like: First Name, Second Name, Vehicle Make, Vehicle Model, Vehicle Year
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing vehicles CSV at: {path}")

    recs: List[Dict] = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            _ = _get(row, "__warmup__")

            make = _norm(_get(row, "make", "vehicle make"))
            model = _norm(_get(row, "model", "vehicle model"))
            vehicle_model = (make + " " + model).strip() if make or model else _norm(_get(row, "vehicle", "car model", "model name") or "Unknown")

            year_raw = _norm(_get(row, "year", "vehicle year", "vehicle_year") or "")
            try:
                year = int(year_raw) if year_raw else None
            except ValueError:
                year = None

            first_v, last_v = _extract_vehicle_name(row)
            postcode_v = _norm(_get(row, "address_postcode", "postcode") or "")

            cust_id_raw = _norm(_get(row, "customer_id", "customerid", "customer id") or "")
            try:
                cust_id = int(cust_id_raw) if cust_id_raw else None
            except ValueError:
                cust_id = None

            recs.append({
                "model": vehicle_model or "Unknown",
                "year": year,
                "customer_id": cust_id,
                "first_name": first_v,
                "last_name": last_v,
                "postcode": postcode_v,
            })

    print(f"   📊 Extracted {len(recs)} vehicles from CSV")
    return recs

def read_extras_txt(path: str) -> List[str]:
    if not os.path.exists(path):
        print("   ℹ️  No extras/notes file found; skipping.")
        return []
    lines = []
    for t in _read_lines_any_encoding(path):
        t = t.strip()
        if t:
            lines.append(t)
    print(f"   📊 Extracted {len(lines)} free-text lines from extras")
    return lines

# =====================================================
# 5) TRANSFORMATION & UNIFICATION
# =====================================================

def unify_records(customers: List[Dict], vehicles: List[Dict], policies: List[Dict], extras_lines: Optional[List[str]] = None) -> Dict[int, Dict]:
    """
    Build a unified map {customer_id: {customer fields..., vehicles:[], policies:[], notes:[]}}
    """
    by_exact: Dict[Tuple[str, str, str], int] = {}
    by_relaxed: Dict[Tuple[str, str, str], int] = {}
    by_full: Dict[str, List[int]] = {}               

    unified: Dict[int, Dict] = {}

    for c in customers:
        cid = c["id"]
        first = c.get("first_name", "")
        last = c.get("last_name", "")
        pc = c.get("address_postcode", "")
        key_exact = (first, last, (pc or "").upper())
        key_relax = (first, last, "")
        fullk = _full_key(first, last)

        by_exact[key_exact] = cid
        if key_relax not in by_relaxed:
            by_relaxed[key_relax] = cid
        by_full.setdefault(fullk, []).append(cid)

        unified[cid] = {**c, "vehicles": [], "policies": [], "notes": []}

    unmatched_vehicles = 0
    matched_vehicles = 0
    for v in vehicles:
        first_v, last_v = v["first_name"], v["last_name"]
        pc_v = (v.get("postcode") or "").upper()

        target_id = None
        if first_v or last_v:
            if first_v and last_v:
                k1 = (first_v, last_v, pc_v)
                target_id = by_exact.get(k1)
                if target_id is None and pc_v:
                    k1b = (first_v, last_v, "")
                    target_id = by_exact.get(k1b)
                if target_id is None:
                    k2 = (first_v, last_v, "")
                    target_id = by_relaxed.get(k2)
                if target_id is None:
                    ids = by_full.get(_full_key(first_v, last_v))
                    if ids:
                        target_id = ids[0]

        if target_id is None:
            unmatched_vehicles += 1
            continue

        unified[target_id]["vehicles"].append({"model": v["model"], "year": v["year"]})
        matched_vehicles += 1

    created_from_policies = 0
    for p in policies:
        first, last, pc = p["customer_lookup"]
        cid = by_exact.get((first, last, pc)) or by_relaxed.get((first, last, ""))
        if cid is None:
            ids = by_full.get(_full_key(first, last))
            cid = ids[0] if ids else None

        if cid is None:
            gen_id = _deterministic_id_within_range(first or "unknown", last or "unknown", pc or "unknown")
            if gen_id not in unified:
                by_exact[(first, last, pc)] = gen_id
                by_relaxed.setdefault((first, last, ""), gen_id)
                by_full.setdefault(_full_key(first, last), []).append(gen_id)
                placeholder = {
                    "id": gen_id,
                    "first_name": first or None,
                    "last_name": last or None,
                    "marital_status": None,
                    "salary": None,
                    "address": pc or None,
                    "address_postcode": pc or None,
                    "vehicles": [],
                    "policies": [],
                    "notes": []
                }
                unified[gen_id] = placeholder
                created_from_policies += 1
            cid = gen_id

        unified[cid]["policies"].append({
            "start_date": p["start_date"],
            "end_date": p["end_date"],
            "monthly_payment": p["monthly_payment"],
            "payment_frequency": p["payment_frequency"]
        })

    if unmatched_vehicles:
        print(f"   ⚠  Vehicles not matched to any customer: {unmatched_vehicles}")
    print(f"   ✅ Vehicles matched to customers: {matched_vehicles}")
    if created_from_policies:
        print(f"   ℹ️  Created {created_from_policies} placeholder customer(s) from policies-only records")

    # Attach free-text notes by simple full-name scan
    unmatched_notes = 0
    if extras_lines:
        name_to_ids: Dict[str, List[int]] = {}
        for cid, c in unified.items():
            full = _full_key(c.get('first_name') or "", c.get('last_name') or "")
            if full:
                name_to_ids.setdefault(full, []).append(cid)

        for line in extras_lines:
            attached = False
            ln = _norm_name(line)
            for full_name, ids in name_to_ids.items():
                if re.search(rf"\b{re.escape(full_name)}\b", ln):
                    for cid in ids:
                        unified[cid]["notes"].append(line)
                    attached = True
            if not attached:
                unmatched_notes += 1

    if unmatched_notes:
        print(f"   ℹ️  Notes lines not attached to any customer: {unmatched_notes}")

    return unified

# =====================================================
# 6) LOAD INTO DATABASE
# =====================================================

def _fallback_agent_code():
    try:
        with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES LIKE 'AGENTS'")
                if not cur.fetchone():
                    return None
                cur.execute("SELECT AGENT_CODE FROM AGENTS LIMIT 1")
                r = cur.fetchone()
                return r[0] if r else None
    except Exception:
        return None

def load_to_db(unified_dict: Dict[int, Dict]):
    cols_meta = {c["Field"]: c for c in CUSTOMER_COLS}
    cols_set = set(cols_meta.keys())
    required = _required_cols(CUSTOMER_COLS) - {PK_COL}

    first_col = _pick_column(cols_set, ["FIRST_NAME","first_name","FirstName","FNAME","fname"])
    last_col  = _pick_column(cols_set, ["LAST_NAME","last_name","LastName","LNAME","lname"])
    full_col  = _pick_column(cols_set, ["FULL_NAME","full_name","NAME","name","CUST_NAME"])

    addr_line_col = _pick_column(cols_set, ["ADDRESS_LINE","address_line","ADDRESS","address","Address","ADDR","addr","WORKING_AREA"])
    postal_col    = _pick_column(cols_set, ["POSTAL_CODE","postal_code","ZIP","zip","postcode","address_postcode"])
    city_col      = _pick_column(cols_set, ["CITY","city","CUST_CITY"])
    country_col   = _pick_column(cols_set, ["COUNTRY","country","CUST_COUNTRY"])

    email_col = _pick_column(cols_set, ["EMAIL","email"])
    phone_col = _pick_column(cols_set, ["PHONE","phone","PHONE_NO","PHONE_NUMBER"])
    marital_col = _pick_column(cols_set, ["MARITAL_STATUS","marital_status","MaritalStatus"])
    sal_col   = _pick_column(cols_set, ["SALARY","salary","AnnualSalary","annual_salary","OPENING_AMT"])
    grade_col = _pick_column(cols_set, ["GRADE","grade"])
    agent_col = _pick_column(cols_set, ["AGENT_CODE","agent_code"])

    fallback_agent = _fallback_agent_code()

    with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=True) as conn:
        with conn.cursor() as cur:
            for cid, data in unified_dict.items():
                row = {}

                if not _PK_AUTO:
                    safe_id = cid
                    if safe_id < max(1, _PK_MIN): safe_id = max(1, _PK_MIN)
                    if safe_id > _PK_MAX:         safe_id = _PK_MAX
                    row[PK_COL] = int(safe_id)

                fn = data.get('first_name') or ""
                ln = data.get('last_name') or ""
                fn_t = _title_safe(fn)
                ln_t = _title_safe(ln)
                full_t = _title_safe(f"{fn} {ln}".strip()) or "Unknown"

                if full_col:  row[full_col]  = full_t
                if first_col: row[first_col] = fn_t or "Unknown"
                if last_col:  row[last_col]  = ln_t or "Unknown"

                if marital_col:
                    row[marital_col] = data.get('marital_status') or "Unknown"

                if sal_col:
                    sal_val = data.get('salary')
                    row[sal_col] = sal_val if sal_val is not None else 0

                postcode = data.get('address_postcode') or data.get('address')
                if addr_line_col:
                    row[addr_line_col] = data.get('address') or "Unknown"
                if postal_col:
                    row[postal_col] = postcode or "Unknown"

                if city_col:
                    row[city_col] = "Unknown"
                if country_col:
                    row[country_col] = "UK"

                if email_col:
                    row[email_col] = "Unknown"
                if phone_col:
                    row[phone_col] = ""

                if grade_col:
                    row[grade_col] = 1
                if agent_col:
                    row[agent_col] = fallback_agent

                for col in required:
                    if col not in row:
                        t = cols_meta[col]["Type"].lower()
                        if _is_numeric(t):
                            row[col] = 0
                        elif "date" in t:
                            row[col] = "1970-01-01"
                        else:
                            row[col] = "Unknown"

                insert_cols = list(row.keys())
                insert_vals = [row[c] for c in insert_cols]
                placeholders = ", ".join(["%s"] * len(insert_cols))
                col_list = ", ".join(f"`{c}`" for c in insert_cols)
                update_list = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in insert_cols if c != PK_COL)

                sql = f"INSERT INTO `{CUSTOMER_TABLE_NAME}` ({col_list}) VALUES ({placeholders})"
                if update_list and not _PK_AUTO:
                    sql += f" ON DUPLICATE KEY UPDATE {update_list}"
                cur.execute(sql, insert_vals)

    print(f"✅ Loaded/updated {len(unified_dict)} customers into {CUSTOMER_TABLE_NAME}.")

# =====================================================
# 7) DISPLAY RESULTS
# =====================================================

def display_results():
    with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{CUSTOMER_TABLE_NAME}` LIMIT 50")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

            print("\n" + "="*50)
            print(f"{CUSTOMER_TABLE_NAME} TABLE CONTENTS")
            print("="*50)
            print(f"\n📊 Rows shown: {len(rows)}")
            for r in rows:
                line = " | ".join(f"{c}={v}" for c, v in zip(cols, r))
                print(f" - {line}")

# =====================================================
# 8) MAIN
# =====================================================

def main():
    print("🚀 Starting CarInsur ETL Process")
    print("="*50)

    print("\n1️⃣ Testing database connection...")
    if not test_connection():
        print("❌ Cannot proceed without database connection. Exiting.")
        return

    print("\n2️⃣ Reading data files from ./data ...")
    try:
        db.bind(provider='mysql', host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME)
        db.generate_mapping(create_tables=False)
        print(f"✅ Database mapping verified (no new tables created). Using table: {CUSTOMER_TABLE_NAME}")
    except Exception as e:
        print(f"❌ Failed to set up database mapping: {e}")
        return

    print("\n3️⃣ Extracting data from files...")
    try:
        customers = read_customers_xml(CUSTOMERS_XML)
        vehicles = read_vehicles_csv(VEHICLES_CSV)
        policies = read_policies_json(POLICIES_JSON)
        extras = read_extras_txt(EXTRAS_TXT)
    except Exception as e:
        print(f"❌ Failed to extract data: {e}")
        return

    print("\n4️⃣ Transforming and unifying data...")
    unified = unify_records(customers, vehicles, policies, extras)
    print(f"   ✅ Unified {len(unified)} customer records")

    print("\n5️⃣ Loading data into database...")
    try:
        load_to_db(unified)
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return

    print("\n6️⃣ Displaying results...")
    try:
        display_results()
    except Exception as e:
        print(f"❌ Failed to display results: {e}")
        return

    print("\n" + "="*50)
    print("🎉 ETL Process completed successfully!")
    print("="*50)

if __name__ == '__main__':
    main()
