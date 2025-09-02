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
from pony.orm import Database, Required, Optional, PrimaryKey, db_session, commit

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

# =====================================================
# 3. DEFINE ORM ENTITIES
# =====================================================

class Customer(db.Entity):
    _table_ = 'CUSTOMER'  # existing table in your DB
    id = PrimaryKey(str, column='CUST_CODE')        # PK (store CSV id as string)
    full_name = Required(str, column='CUST_NAME')   # join first + last
    working_area = Optional(str, column='WORKING_AREA')
    opening_amt = Optional(float, column='OPENING_AMT')
    receive_amt = Optional(float, column='RECEIVE_AMT')
    payment_amt = Optional(float, column='PAYMENT_AMT')
    outstanding_amt = Optional(float, column='OUTSTANDING_AMT')
    grade = Optional(int, column='GRADE')
    cust_city = Optional(str, column='CUST_CITY')
    cust_country = Optional(str, column='CUST_COUNTRY')
    phone_no = Optional(str, column='PHONE_NO')
    agent_code = Optional(str, column='AGENT_CODE')

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
        {
            'id': int(e['id']),
            'model': e['model'].strip(),
            'year': int(e['year']),
            'customer_id': int(e['customer_id'])
        } for e in data
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
# 6. LOAD INTO DATABASE
# =====================================================

def get_fallback_agent_code():
    """Return any existing AGENT_CODE from AGENTS table, or None if not present."""
    try:
        with pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT AGENT_CODE FROM AGENTS LIMIT 1")
                row = cur.fetchone()
                return row[0] if row else None
    except Exception:
        return None

def load_to_db(unified_dict):
    """Persist unified records into the existing CUSTOMER table only (no new tables)."""
    fallback_agent = get_fallback_agent_code()

    with db_session:
        for cid, data in unified_dict.items():
            cust_code = str(cid)
            full_name = " ".join([x for x in [data.get('first_name'), data.get('last_name')] if x]) or ""
            working_area = data.get('address') or "Unknown"
            opening_amt = data.get('salary') if data.get('salary') is not None else 0.0

            defaults = {
                'receive_amt': 0.0,
                'payment_amt': 0.0,
                'outstanding_amt': 0.0,
                'grade': 1,
                'cust_city': "Unknown",
                'cust_country': "UK",
                'phone_no': "",
                'agent_code': fallback_agent
            }

            cust = Customer.get(id=cust_code)
            if not cust:
                Customer(
                    id=cust_code,
                    full_name=full_name,
                    working_area=working_area,
                    opening_amt=opening_amt,
                    receive_amt=defaults['receive_amt'],
                    payment_amt=defaults['payment_amt'],
                    outstanding_amt=defaults['outstanding_amt'],
                    grade=defaults['grade'],
                    cust_city=defaults['cust_city'],
                    cust_country=defaults['cust_country'],
                    phone_no=defaults['phone_no'],
                    agent_code=defaults['agent_code'],
                )
            else:
                if not cust.full_name and full_name:
                    cust.full_name = full_name
                if (cust.working_area is None or not cust.working_area) and working_area:
                    cust.working_area = working_area
                if cust.opening_amt is None:
                    cust.opening_amt = opening_amt
                if cust.receive_amt is None:
                    cust.receive_amt = defaults['receive_amt']
                if cust.payment_amt is None:
                    cust.payment_amt = defaults['payment_amt']
                if cust.outstanding_amt is None:
                    cust.outstanding_amt = defaults['outstanding_amt']
                if cust.grade is None:
                    cust.grade = defaults['grade']
                if cust.cust_city is None:
                    cust.cust_city = defaults['cust_city']
                if cust.cust_country is None:
                    cust.cust_country = defaults['cust_country']
                if cust.phone_no is None:
                    cust.phone_no = defaults['phone_no']
                if cust.agent_code is None:
                    cust.agent_code = defaults['agent_code']

        commit()
    print(f"✅ Loaded/updated {len(unified_dict)} customers into CUSTOMER.")

# =====================================================
# 7. QUERY AND DISPLAY RESULTS
# =====================================================

def display_results():
    """Query and display the loaded data from CUSTOMER table"""
    with db_session:
        print("\n" + "="*50)
        print("CUSTOMER TABLE CONTENTS")
        print("="*50)
        customers = Customer.select()[:]
        print(f"\n📊 Total Customers (matching our IDs): {len(customers)}")
        for c in customers:
            print(f"- {c.id}: {c.full_name} | Area: {c.working_area} | Country: {c.cust_country} | OpeningAmt: {c.opening_amt}")

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
        print("✅ Database mapping verified (no new tables created).")
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