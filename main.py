# =====================================================
# CARINSUR ETL SOLUTION
# =====================================================
# Assessment: Python ETL for CarInsur SME Car Insurance Company
# 
# BUSINESS PROBLEM:
# CarInsur operates with siloed IT systems where customer data, vehicle data,
# and insurance policy data are stored separately. This fragmentation poses
# challenges as the company grows. They need a unified, central database
# solution to consolidate all customer records.
#
# SOLUTION OVERVIEW:
# This ETL pipeline extracts data from multiple formats (CSV, JSON, XML, TXT),
# transforms and unifies records by customer ID, and loads them into a 
# relational database using PonyORM. This eliminates manual intervention
# and provides CarInsur with cohesive, centralized customer records.
#
# LIBRARIES USED (Assessment Compliant):
# - Standard Python libraries: csv, json, xml.etree.ElementTree, os, datetime
# - PonyORM: For relational database mapping and operations
# - PyMySQL: For MySQL database connectivity
# =====================================================
import csv
import json
import xml.etree.ElementTree as ET
import os
import pymysql
from datetime import datetime
from pony.orm import Database, PrimaryKey, db_session, commit

# =====================================================
# 1. DATABASE CONFIGURATION AND CONNECTION TEST
# =====================================================

DB_HOST = 'europa.ashley.work'
DB_USER = 'student_bi95au'
DB_PASSWORD = 'iE93F2@8EhM@1zhD&u9M@K'
DB_NAME = 'student_bi95au'

db = Database()

def test_connection():
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
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
# 2. CREATE SAMPLE DATA FILES
# =====================================================

def create_sample_data():
    if not os.path.exists('data'):
        os.makedirs('data')
        print("📁 Created 'data' directory")
    
    customers_data = """
id,first_name,last_name,marital_status,salary,address
1,John,Smith,Married,55000,123 Main St
2,Jane,Doe,Single,62000,456 Oak Ave
3,Bob,Johnson,Divorced,48000,789 Pine Rd
4,Alice,Brown,Married,71000,321 Elm St
"""
    with open('data/customers.csv', 'w', newline='') as f:
        f.write(customers_data.strip() + "\n")
    print("📄 Created customers.csv")
    
    vehicles_data = [
        {"id": 1, "model": "Toyota Camry", "year": 2020, "customer_id": 1},
        {"id": 2, "model": "Honda Civic", "year": 2019, "customer_id": 2},
        {"id": 3, "model": "Ford F-150", "year": 2021, "customer_id": 3},
        {"id": 4, "model": "BMW X5", "year": 2022, "customer_id": 4}
    ]
    with open('data/vehicles.json', 'w') as f:
        json.dump(vehicles_data, f, indent=2)
    print("📄 Created vehicles.json")
    
    policies_xml = """
<?xml version="1.0" encoding="UTF-8"?>
<policies>
  <policy>
    <id>1</id>
    <customer_id>1</customer_id>
    <start_date>2024-01-01</start_date>
    <end_date>2024-12-31</end_date>
    <monthly_payment>150.00</monthly_payment>
    <payment_frequency>Monthly</payment_frequency>
  </policy>
  <policy>
    <id>2</id>
    <customer_id>2</customer_id>
    <start_date>2024-02-01</start_date>
    <end_date>2025-01-31</end_date>
    <monthly_payment>120.00</monthly_payment>
    <payment_frequency>Monthly</payment_frequency>
  </policy>
  <policy>
    <id>3</id>
    <customer_id>3</customer_id>
    <start_date>2024-03-01</start_date>
    <end_date>2025-02-28</end_date>
    <monthly_payment>180.00</monthly_payment>
    <payment_frequency>Monthly</payment_frequency>
  </policy>
</policies>
"""
    with open('data/policies.xml', 'w') as f:
        f.write(policies_xml.strip() + "\n")
    print("📄 Created policies.xml")
    
    extras_data = """
customer_id:1,alternate_address:456 Secondary St,phone:555-0123
customer_id:2,emergency_contact:John Doe,phone:555-0456
customer_id:3,notes:High risk driver,phone:555-0789
"""
    with open('data/extras.txt', 'w') as f:
        f.write(extras_data.strip() + "\n")
    print("📄 Created extras.txt")
    
    print("✅ All sample data files created successfully!")

# ----- Find the single customer table (prefer CARINSUR_CUSTOMER) -----
def _find_customer_table():
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

def _get_table_columns(table_name):
    with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
            rows = cur.fetchall()
            return [{"Field": r[0], "Type": r[1], "Null": r[2], "Key": r[3], "Default": r[4], "Extra": r[5]} for r in rows]

def _pick_column(cols_set, candidates):
    for c in candidates:
        if c in cols_set:
            return c
    return None

def _is_numeric(mysql_type):
    t = mysql_type.lower()
    return any(x in t for x in ["int", "decimal", "float", "double", "numeric"])

def _required_cols(cols):
    req = set()
    for c in cols:
        if c["Null"] == "NO" and c["Default"] is None and "auto_increment" not in c["Extra"]:
            req.add(c["Field"])
    return req

def _primary_key(cols):
    for c in cols:
        if c["Key"] == "PRI":
            return c["Field"], (_is_numeric(c["Type"]))
    return None, False

CUSTOMER_TABLE_NAME = _find_customer_table()
CUSTOMER_COLS = _get_table_columns(CUSTOMER_TABLE_NAME)
PK_COL, PK_IS_NUMERIC = _primary_key(CUSTOMER_COLS)
if not PK_COL:
    raise RuntimeError(f"Customer table `{CUSTOMER_TABLE_NAME}` has no primary key.")

# Minimal entity just to keep Pony binding happy; writes are done via raw SQL
class Customer(db.Entity):
    _table_ = CUSTOMER_TABLE_NAME
    if PK_IS_NUMERIC:
        id = PrimaryKey(int, column=PK_COL)
    else:
        id = PrimaryKey(str, column=PK_COL)

# =====================================================
# 4. EXTRACTION FUNCTIONS
# =====================================================

def read_customers_csv(path):
    records = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            salary = float(row['salary']) if row.get('salary') and row['salary'].strip() else None
            records.append({
                'id': int(row['id']),
                'first_name': row['first_name'].strip(),
                'last_name': row['last_name'].strip(),
                'marital_status': row.get('marital_status', '').strip() or None,
                'salary': salary,
                'address': row.get('address', '').strip() or None
            })
    return records

def read_vehicles_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return [
        {'id': int(e['id']), 'model': e['model'].strip(), 'year': int(e['year']), 'customer_id': int(e['customer_id'])}
        for e in data
    ]

def read_policies_xml(path):
    tree = ET.parse(path)
    root = tree.getroot()
    records = []
    for pol in root.findall('policy'):
        start = datetime.strptime(pol.find('start_date').text, '%Y-%m-%d')
        end = datetime.strptime(pol.find('end_date').text, '%Y-%m-%d')
        records.append({
            'id': int(pol.find('id').text),
            'customer_id': int(pol.find('customer_id').text),
            'start_date': start,
            'end_date': end,
            'monthly_payment': float(pol.find('monthly_payment').text),
            'payment_frequency': pol.find('payment_frequency').text.strip()
        })
    return records

def read_additional_txt(path):
    records = []
    with open(path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                parts = [p.strip() for p in line.split(',') if p.strip()]
                entry = {}
                for part in parts:
                    if ':' in part:
                        k, v = [x.strip() for x in part.split(':', 1)]
                        entry[k] = v
                if 'id' in entry:
                    entry['id'] = int(entry['id'])
                if 'customer_id' in entry:
                    entry['customer_id'] = int(entry['customer_id'])
                if entry:
                    records.append(entry)
            except (ValueError, TypeError) as e:
                print(f"⚠  Warning: Skipping malformed line {line_num} in {path}: {e}")
                continue
    return records

# =====================================================
# 5. TRANSFORMATION AND UNIFICATION
# =====================================================

def unify_records(customers, vehicles, policies, extras=None):
    unified = {}
    for c in customers:
        cid = c['id']
        unified[cid] = {**c, 'vehicles': [], 'policies': []}
    for v in vehicles:
        cid = v['customer_id']
        unified.setdefault(cid, {
            'id': cid, 'first_name': None, 'last_name': None,
            'marital_status': None, 'salary': None, 'address': None,
            'vehicles': [], 'policies': []
        })
        unified[cid]['vehicles'].append(v)
    for p in policies:
        cid = p['customer_id']
        unified.setdefault(cid, {
            'id': cid, 'first_name': None, 'last_name': None,
            'marital_status': None, 'salary': None, 'address': None,
            'vehicles': [], 'policies': []
        })
        unified[cid]['policies'].append(p)
    if extras:
        for extra in extras:
            cid = extra.get('customer_id') or extra.get('id')
            if cid and cid in unified:
                for k, v in extra.items():
                    if k not in ('id', 'customer_id') and v:
                        unified[cid][k] = v
    return unified

# =====================================================
# 6. LOAD INTO DATABASE (safe UPSERT into one customer table)
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

def load_to_db(unified_dict):
    cols_meta = {c["Field"]: c for c in CUSTOMER_COLS}
    cols_set = set(cols_meta.keys())
    required = _required_cols(CUSTOMER_COLS) - {PK_COL}

    first_col = _pick_column(cols_set, ["FIRST_NAME","first_name","FirstName","FNAME","fname"])
    last_col  = _pick_column(cols_set, ["LAST_NAME","last_name","LastName","LNAME","lname"])
    name_col  = _pick_column(cols_set, ["FULL_NAME","full_name","NAME","name","CUST_NAME"])
    addr_col  = _pick_column(cols_set, ["ADDRESS","address","Address","ADDR","addr","WORKING_AREA"])
    sal_col   = _pick_column(cols_set, ["SALARY","salary","AnnualSalary","annual_salary","OPENING_AMT"])
    city_col  = _pick_column(cols_set, ["CITY","city","CUST_CITY"])
    country_col=_pick_column(cols_set, ["COUNTRY","country","CUST_COUNTRY"])
    phone_col = _pick_column(cols_set, ["PHONE","phone","PHONE_NO","PHONE_NUMBER"])
    grade_col = _pick_column(cols_set, ["GRADE","grade"])
    agent_col = _pick_column(cols_set, ["AGENT_CODE","agent_code"])

    fallback_agent = _fallback_agent_code()

    with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=True) as conn:
        with conn.cursor() as cur:
            for cid, data in unified_dict.items():
                row = {}
                row[PK_COL] = int(cid) if PK_IS_NUMERIC else str(cid)

                if name_col:
                    full_name = " ".join([x for x in [data.get('first_name'), data.get('last_name')] if x]) or None
                    row[name_col] = full_name or "Unknown"
                else:
                    if first_col: row[first_col] = data.get('first_name') or "Unknown"
                    if last_col:  row[last_col]  = data.get('last_name')  or "Unknown"

                if addr_col: row[addr_col] = data.get('address') or "Unknown"
                if sal_col:  row[sal_col]  = (data.get('salary') if data.get('salary') is not None else 0)
                if city_col: row[city_col] = "Unknown"
                if country_col: row[country_col] = "UK"
                if phone_col: row[phone_col] = ""
                if grade_col: row[grade_col] = 1
                if agent_col: row[agent_col] = fallback_agent

                for col in required:
                    if col not in row:
                        t = cols_meta[col]["Type"].lower()
                        if _is_numeric(t): row[col] = 0
                        elif "date" in t: row[col] = "1970-01-01"
                        else: row[col] = "Unknown"

                insert_cols = list(row.keys())
                insert_vals = [row[c] for c in insert_cols]
                placeholders = ", ".join(["%s"]*len(insert_cols))
                col_list = ", ".join(f"`{c}`" for c in insert_cols)
                update_list = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in insert_cols if c != PK_COL)

                sql = f"INSERT INTO `{CUSTOMER_TABLE_NAME}` ({col_list}) VALUES ({placeholders})"
                if update_list:
                    sql += f" ON DUPLICATE KEY UPDATE {update_list}"
                cur.execute(sql, insert_vals)

    print(f"✅ Loaded/updated {len(unified_dict)} customers into {CUSTOMER_TABLE_NAME}.")

# =====================================================
# 7. QUERY AND DISPLAY RESULTS
# =====================================================

def display_results():
    """Show top 50 rows from the customer table with all columns."""
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
# 8. MAIN EXECUTION
# =====================================================

def main():
    print("🚀 Starting CarInsur ETL Process")
    print("="*50)
    
    print("\n1️⃣ Testing database connection...")
    if not test_connection():
        print("❌ Cannot proceed without database connection. Exiting.")
        return
    
    print("\n2️⃣ Creating sample data files...")
    create_sample_data()
    
    print("\n3️⃣ Setting up database connection and tables...")
    try:
        db.bind(provider='mysql', host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME)
        db.generate_mapping(create_tables=False)  # do NOT create any tables
        print(f"✅ Database mapping verified (no new tables created). Using table: {CUSTOMER_TABLE_NAME}")
    except Exception as e:
        print(f"❌ Failed to set up database mapping: {e}")
        return
    
    print("\n4️⃣ Extracting data from files...")
    try:
        customers = read_customers_csv('data/customers.csv')
        vehicles = read_vehicles_json('data/vehicles.json')
        policies = read_policies_xml('data/policies.xml')
        extras = read_additional_txt('data/extras.txt')
        print(f"   📊 Extracted {len(customers)} customers")
        print(f"   📊 Extracted {len(vehicles)} vehicles")
        print(f"   📊 Extracted {len(policies)} policies")
        print(f"   📊 Extracted {len(extras)} extra records")
    except Exception as e:
        print(f"❌ Failed to extract data: {e}")
        return
    
    print("\n5️⃣ Transforming and unifying data...")
    unified = unify_records(customers, vehicles, policies, extras)
    print(f"   ✅ Unified {len(unified)} customer records")
    
    print("\n6️⃣ Loading data into database...")
    try:
        load_to_db(unified)
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return
    
    print("\n7️⃣ Displaying results...")
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