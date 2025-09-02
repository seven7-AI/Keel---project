import csv
import json
import xml.etree.ElementTree as ET
import os
from datetime import datetime
from pony.orm import Database, Required, Optional, Set, PrimaryKey, db_session, commit, select

# -------------------------
# 0) CONFIG
# -------------------------
DB_HOST = 'europa.ashley.work'
DB_USER = 'student_bi95au'
DB_PASSWORD = 'iE93F2@8EhM@1zhD&u9M@K'
DB_NAME = 'student_bi95au'

# Match EXISTING table names in phpMyAdmin to avoid creation/case issues
TBL_CUSTOMER = 'CUSTOMER'
TBL_VEHICLE  = 'VEHICLES'
TBL_POLICY   = 'POLICIES'

DATA_DIR = 'data'
OUTPUT_DIR = 'output'

# Initialize PonyORM Database
db = Database()

# =====================================================
# 1) ENTITIES (map to EXISTING phpMyAdmin tables/columns)
# =====================================================
class Customer(db.Entity):
    _table_ = 'CUSTOMER'  # keep as your actual table name (case sensitive)
    id = PrimaryKey(int, column='CUSTOMER_ID')     # <-- map to your PK column
    first_name = Optional(str, column='FIRST_NAME')
    last_name = Optional(str, column='LAST_NAME')
    marital_status = Optional(str, column='MARITAL_STATUS')
    salary = Optional(float, column='SALARY')
    address = Optional(str, column='ADDRESS')
    vehicles = Set('Vehicle')
    policies = Set('Policy')

class Vehicle(db.Entity):
    _table_ = 'VEHICLES'  # adjust if your actual name differs
    id = PrimaryKey(int, column='VEHICLE_ID')
    model = Required(str, column='MODEL')
    year = Required(int, column='YEAR')
    # Foreign key to CUSTOMER table:
    customer = Required(Customer, column='CUSTOMER_ID')

class Policy(db.Entity):
    _table_ = 'POLICIES'  # adjust if your actual name differs
    id = PrimaryKey(int, column='POLICY_ID')
    start_date = Required(datetime, column='START_DATE')
    end_date = Required(datetime, column='END_DATE')
    monthly_payment = Required(float, column='MONTHLY_PAYMENT')
    payment_frequency = Required(str, column='PAYMENT_FREQUENCY')
    # Foreign key to CUSTOMER table:
    customer = Required(Customer, column='CUSTOMER_ID')

# =====================================================
# 2) DATA CREATION (idempotent — only if missing)
# =====================================================
def create_sample_data_if_missing():
    os.makedirs(DATA_DIR, exist_ok=True)
    created_any = False

    cust_path = os.path.join(DATA_DIR, 'customers.csv')
    if not os.path.exists(cust_path):
        customers_data = (
            "id,first_name,last_name,marital_status,salary,address\n"
            "1,John,Smith,Married,55000,123 Main St\n"
            "2,Jane,Doe,Single,62000,456 Oak Ave\n"
            "3,Bob,Johnson,Divorced,48000,789 Pine Rd\n"
            "4,Alice,Brown,Married,71000,321 Elm St\n"
        )
        with open(cust_path, 'w', newline='', encoding='utf-8') as f:
            f.write(customers_data)
        print("📄 Created customers.csv")
        created_any = True

    veh_path = os.path.join(DATA_DIR, 'vehicles.json')
    if not os.path.exists(veh_path):
        vehicles_data = [
            {"id": 1, "model": "Toyota Camry", "year": 2020, "customer_id": 1},
            {"id": 2, "model": "Honda Civic", "year": 2019, "customer_id": 2},
            {"id": 3, "model": "Ford F-150", "year": 2021, "customer_id": 3},
            {"id": 4, "model": "BMW X5", "year": 2022, "customer_id": 4}
        ]
        with open(veh_path, 'w', encoding='utf-8') as f:
            json.dump(vehicles_data, f, indent=2)
        print("📄 Created vehicles.json")
        created_any = True

    pol_path = os.path.join(DATA_DIR, 'policies.xml')
    if not os.path.exists(pol_path):
        policies_xml = """<?xml version="1.0" encoding="UTF-8"?>
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
</policies>"""
        with open(pol_path, 'w', encoding='utf-8') as f:
            f.write(policies_xml)
        print("📄 Created policies.xml")
        created_any = True

    extra_path = os.path.join(DATA_DIR, 'extras.txt')
    if not os.path.exists(extra_path):
        extras_data = (
            "customer_id:1,alternate_address:456 Secondary St,phone:555-0123\n"
            "customer_id:2,emergency_contact:John Doe,phone:555-0456\n"
            "customer_id:3,notes:High risk driver,phone:555-0789\n"
        )
        with open(extra_path, 'w', encoding='utf-8') as f:
            f.write(extras_data)
        print("📄 Created extras.txt")
        created_any = True

    if created_any:
        print("✅ Sample data created (only because it was missing).")
    else:
        print("ℹ️ Sample data already present; nothing created.")

# =====================================================
# 3) EXTRACT
# =====================================================
def read_customers_csv(path):
    records = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            salary = float(row['salary']) if row.get('salary') and row['salary'].strip() else None
            records.append({
                'id': int(row['id']),
                'first_name': row.get('first_name', '').strip() or None,
                'last_name': row.get('last_name', '').strip() or None,
                'marital_status': row.get('marital_status', '').strip() or None,
                'salary': salary,
                'address': row.get('address', '').strip() or None
            })
    return records

def read_vehicles_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    records = []
    for entry in data:
        records.append({
            'id': int(entry['id']),
            'model': entry['model'].strip(),
            'year': int(entry['year']),
            'customer_id': int(entry['customer_id'])
        })
    return records

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
                print(f"⚠ Warning: Skipping malformed line {line_num} in {path}: {e}")
                continue
    return records

# =====================================================
# 4) TRANSFORM (unify by customer)
# =====================================================
def unify_records(customers, vehicles, policies, extras=None):
    unified = {}
    for c in customers:
        cid = c['id']
        unified[cid] = {**c, 'vehicles': [], 'policies': []}
    for v in vehicles:
        cid = v['customer_id']
        if cid not in unified:
            unified[cid] = {'id': cid, 'first_name': None, 'last_name': None,
                            'marital_status': None, 'salary': None, 'address': None,
                            'vehicles': [], 'policies': []}
        unified[cid]['vehicles'].append(v)
    for p in policies:
        cid = p['customer_id']
        if cid not in unified:
            unified[cid] = {'id': cid, 'first_name': None, 'last_name': None,
                            'marital_status': None, 'salary': None, 'address': None,
                            'vehicles': [], 'policies': []}
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
# 5) LOAD (no table creation; upsert into existing)
# =====================================================
def load_to_db(unified_dict):
    with db_session:
        for cid, data in unified_dict.items():
            cust = Customer.get(id=cid)
            if not cust:
                cust = Customer(id=cid,
                                first_name=data.get('first_name'),
                                last_name=data.get('last_name'),
                                marital_status=data.get('marital_status'),
                                salary=data.get('salary'),
                                address=data.get('address'))
            else:
                for field in ('first_name','last_name','marital_status','salary','address'):
                    if getattr(cust, field) is None and data.get(field) is not None:
                        setattr(cust, field, data[field])

            for v in data['vehicles']:
                if not Vehicle.get(id=v['id']):
                    Vehicle(id=v['id'], model=v['model'], year=v['year'], customer=cust)

            for p in data['policies']:
                if not Policy.get(id=p['id']):
                    Policy(id=p['id'], start_date=p['start_date'], end_date=p['end_date'],
                           monthly_payment=p['monthly_payment'],
                           payment_frequency=p['payment_frequency'],
                           customer=cust)
        commit()
    print(f"✅ Loaded/updated {len(unified_dict)} customers into database.")

# =====================================================
# 6) VISUALISE RESULTS (console + CSV preview)
# =====================================================
def display_results_and_export_csv():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, 'unified_preview.csv')
    header = [
        'customer_id','first_name','last_name','marital_status','salary','address',
        'vehicle_count','policy_count','example_vehicle','example_policy_monthly'
    ]

    with db_session, open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        print("\n" + "="*60)
        print("DATABASE CONTENTS (Summary)")
        print("="*60)

        for customer in select(c for c in Customer):
            v_count = len(customer.vehicles)
            p_count = len(customer.policies)
            example_v = None
            example_p_amt = None

            if v_count:
                v = list(customer.vehicles)[0]
                example_v = f"{v.year} {v.model}"
            if p_count:
                p = list(customer.policies)[0]
                example_p_amt = p.monthly_payment

            # Console pretty lines
            print(f"👤 {customer.id}: {customer.first_name or ''} {customer.last_name or ''} | "
                  f"Vehicles={v_count} | Policies={p_count}")

            # CSV row
            writer.writerow([
                customer.id, customer.first_name, customer.last_name,
                customer.marital_status, customer.salary, customer.address,
                v_count, p_count, example_v, example_p_amt
            ])

    print(f"\n📝 Preview CSV saved → {csv_path}")

# =====================================================
# 7) MAIN
# =====================================================
def main():
    print("🚀 Starting CarInsur ETL Process (AS1)")
    print("="*50)

    # 1) (Bind first; we don't create tables)
    print("\n1️⃣ Binding to database (no table creation)...")
    try:
        db.bind(provider='mysql', host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME)
        db.generate_mapping(create_tables=False)  # IMPORTANT: do not create tables
        print("✅ Bound to DB; mapping to existing tables only.")
    except Exception as e:
        print(f"❌ Failed to bind/generate mapping: {e}")
        return

    # 2) Lightweight connection test using PonyORM
    print("\n2️⃣ Testing database connection (SELECT 1)...")
    try:
        with db_session:
            _ = select(1)[:]
        print("✅ Database connection OK.")
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return

    # 3) Prepare data (only if missing)
    print("\n3️⃣ Ensuring sample data exists (won't overwrite)...")
    create_sample_data_if_missing()

    # 4) Extract
    print("\n4️⃣ Extracting data...")
    try:
        customers = read_customers_csv(os.path.join(DATA_DIR, 'customers.csv'))
        vehicles  = read_vehicles_json(os.path.join(DATA_DIR, 'vehicles.json'))
        policies  = read_policies_xml(os.path.join(DATA_DIR, 'policies.xml'))
        extras    = read_additional_txt(os.path.join(DATA_DIR, 'extras.txt'))
        print(f"   📊 customers={len(customers)} vehicles={len(vehicles)} policies={len(policies)} extras={len(extras)}")
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return

    # 5) Transform
    print("\n5️⃣ Unifying records...")
    unified = unify_records(customers, vehicles, policies, extras)
    print(f"   ✅ Unified {len(unified)} customer records")

    # 6) Load
    print("\n6️⃣ Loading into existing database tables...")
    try:
        load_to_db(unified)
    except Exception as e:
        print(f"❌ Load failed: {e}")
        return

    # 7) Visualise
    print("\n7️⃣ Visualising output...")
    try:
        display_results_and_export_csv()
    except Exception as e:
        print(f"❌ Visualisation failed: {e}")
        return

    print("\n" + "="*50)
    print("🎉 AS1 ETL completed successfully!")
    print("="*50)

if __name__ == '__main__':
    main()