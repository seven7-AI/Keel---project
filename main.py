# ETL Notebook: Centralising CarInsur Data with PonyORM

# %%
# 1. Imports and Configuration
import csv
import json
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from pony.orm import Database, Required, Optional, Set, db_session, commit

# Database connection parameters (replace <your_student_id>)
DB_HOST = 'europa.ashley.work'
DB_USER = 'student_<your_student_id>'      # e.g., student_bh12xy
DB_PASSWORD = 'iE93F2@8EhM@1zhD&u9M@K'
DB_NAME = 'student_<your_student_id>'

# %%
# 2. Initialize PonyORM Database
db = Database()
# Bind to MySQL
db.bind(provider='mysql', host=DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME)

# %%
# 3. Define ORM Entities
class Customer(db.Entity):
    _table_ = 'customers'
    id = Required(int, unique=True)
    first_name = Required(str)
    last_name = Required(str)
    marital_status = Optional(str)
    salary = Optional(float)
    address = Optional(str)
    vehicles = Set('Vehicle')      # One-to-many: a customer can have many vehicles
    policies = Set('Policy')       # One-to-many: a customer can have many policies

class Vehicle(db.Entity):
    _table_ = 'vehicles'
    id = Required(int, unique=True)
    model = Required(str)
    year = Required(int)
    customer = Required(Customer)

class Policy(db.Entity):
    _table_ = 'policies'
    id = Required(int, unique=True)
    start_date = Required(datetime)
    end_date = Required(datetime)
    monthly_payment = Required(float)
    payment_frequency = Required(str)
    customer = Required(Customer)

# Generate mappings and create tables if they do not exist
db.generate_mapping(create_tables=True)

# %%
# 4. Extraction Functions

def read_customers_csv(path):
    """
    Reads customer data from a CSV file.
    Expected columns: id, first_name, last_name, marital_status, salary, address
    """
    records = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert salary to float if available
            salary = float(row['salary']) if row.get('salary') else None
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
    """
    Reads vehicle data from a JSON file.
    Expected structure: list of {id, model, year, customer_id}
    """
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
    """
    Reads insurance policy data from an XML file.
    Expected structure:
      <policies>
        <policy>
          <id>...</id>
          <customer_id>...</customer_id>
          <start_date>YYYY-MM-DD</start_date>
          <end_date>YYYY-MM-DD</end_date>
          <monthly_payment>...</monthly_payment>
          <payment_frequency>...</payment_frequency>
        </policy>
        ...
      </policies>
    """
    tree = ET.parse(path)
    root = tree.getroot()
    records = []
    for pol in root.findall('policy'):
        # Parse dates
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
    """
    Reads additional key:value data from a TXT file.
    Expected per-line format: key1:value1,key2:value2,...
    Supports adding any extra fields (e.g., alternate address, etc)
    """
    records = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = [p.strip() for p in line.split(',') if p.strip()]
            entry = {}
            for part in parts:
                if ':' in part:
                    k, v = [x.strip() for x in part.split(':', 1)]
                    entry[k] = v
            # Expect entry to contain at least 'id' or 'customer_id'
            if 'id' in entry:
                entry['id'] = int(entry['id'])
            if 'customer_id' in entry:
                entry['customer_id'] = int(entry['customer_id'])
            records.append(entry)
    return records

# %%
# 5. Transformation and Unification

def unify_records(customers, vehicles, policies, extras=None):
    """
    Merge extracted lists into cohesive dicts keyed by customer id.
    extras: list of additional dicts to merge on matching keys.
    """
    unified = {}
    # Start with customers
    for c in customers:
        cid = c['id']
        unified[cid] = {**c, 'vehicles': [], 'policies': []}

    # Attach vehicles
    for v in vehicles:
        cid = v['customer_id']
        if cid in unified:
            unified[cid]['vehicles'].append(v)
        else:
            # Orphaned vehicle: create minimal customer record
            unified[cid] = {'id': cid, 'first_name': None, 'last_name': None,
                            'marital_status': None, 'salary': None, 'address': None,
                            'vehicles': [v], 'policies': []}

    # Attach policies
    for p in policies:
        cid = p['customer_id']
        if cid in unified:
            unified[cid]['policies'].append(p)
        else:
            unified[cid] = {'id': cid, 'first_name': None, 'last_name': None,
                            'marital_status': None, 'salary': None, 'address': None,
                            'vehicles': [], 'policies': [p]}

    # Merge any extras
    if extras:
        for extra in extras:
            cid = extra.get('customer_id') or extra.get('id')
            if cid and cid in unified:
                # Merge all other keys except id and customer_id
                for k, v in extra.items():
                    if k not in ('id', 'customer_id') and v:
                        unified[cid][k] = v
    return unified

# %%
# 6. Load into Database

def load_to_db(unified_dict):
    """
    Persist unified records into the relational database using PonyORM.
    """
    with db_session:
        for cid, data in unified_dict.items():
            # Upsert Customer
            cust = Customer.get(id=cid)
            if not cust:
                cust = Customer(id=cid,
                                first_name=data.get('first_name'),
                                last_name=data.get('last_name'),
                                marital_status=data.get('marital_status'),
                                salary=data.get('salary'),
                                address=data.get('address'))
            else:
                # Update any missing fields
                for field in ('first_name','last_name','marital_status','salary','address'):
                    if getattr(cust, field) is None and data.get(field):
                        setattr(cust, field, data[field])

            # Vehicles
            for v in data['vehicles']:
                if not Vehicle.get(id=v['id']):
                    Vehicle(id=v['id'], model=v['model'], year=v['year'], customer=cust)

            # Policies
            for p in data['policies']:
                if not Policy.get(id=p['id']):
                    Policy(id=p['id'], start_date=p['start_date'],
                           end_date=p['end_date'], monthly_payment=p['monthly_payment'],
                           payment_frequency=p['payment_frequency'], customer=cust)
        commit()
    print(f"Loaded {len(unified_dict)} customers into database.")

# %%
# 7. Main Execution
if __name__ == '__main__':
    # File paths (update to your actual file locations)
    customers_file = 'data/customers.csv'
    vehicles_file = 'data/vehicles.json'
    policies_file = 'data/policies.xml'
    extras_file = 'data/extras.txt'

    # Extract
    custs = read_customers_csv(customers_file)
    vehs = read_vehicles_json(vehicles_file)
    pols = read_policies_xml(policies_file)
    extras = read_additional_txt(extras_file)

    # Transform / Unify
    unified = unify_records(custs, vehs, pols, extras)

    # Load
    load_to_db(unified)
