import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
from datetime import datetime

# --- Load and Clean Data ---

# Load the CSV
df = pd.read_csv('sales_100.csv')

# Rename columns
df.rename(columns={
    'Order ID': 'order_id',
    'Order Date': 'sale_date',
    'TotalRevenue': 'amount'
}, inplace=True)

# Check for missing or invalid order_id
if 'order_id' not in df.columns:
    print("order_id column is missing; adding a placeholder.")
    df['order_id'] = 0
else:
    print("order_id column exists.")

# Clean data
df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce', downcast='integer')
df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce').dt.date
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

# Drop rows with missing data
df = df.dropna(subset=['order_id', 'sale_date', 'amount'])

# Debug: Print the cleaned data
print("Cleaned DataFrame:")
print(df.head())
print(df.dtypes)  # Print data types for verification

# --- Connect to Cassandra ---

cloud_config = {'secure_connect_bundle': 'secure-connect-bigdataproject.zip'}
with open("bigdataproject-token.json") as f:
    secrets = json.load(f)

auth_provider = PlainTextAuthProvider(secrets["clientId"], secrets["secret"])
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Set keyspace
session.set_keyspace('salesdata')

# --- Create Tables ---

# Bronze Table
session.execute("""
CREATE TABLE IF NOT EXISTS bronze_table (
    order_id INT PRIMARY KEY,
    sale_date DATE,
    amount FLOAT
)
""")
print("Bronze table created successfully.")

# Silver Table
session.execute("""
CREATE TABLE IF NOT EXISTS silver_table (
    sale_id UUID PRIMARY KEY,
    order_id INT,
    sale_date DATE,
    amount FLOAT
)
""")
print("Silver table created successfully.")

# Gold Table 1
session.execute("""
CREATE TABLE IF NOT EXISTS gold_table1 (
    order_id INT PRIMARY KEY,
    total_sales FLOAT
)
""")
print("Gold Table 1 created successfully.")

# Gold Table 2
session.execute("""
CREATE TABLE IF NOT EXISTS gold_table2 (
    sale_date DATE PRIMARY KEY,
    total_sales FLOAT
)
""")
print("Gold Table 2 created successfully.")

# Gold Table 3
session.execute("""
CREATE TABLE IF NOT EXISTS gold_table3 (
    region TEXT PRIMARY KEY,
    total_sales FLOAT
)
""")
print("Gold Table 3 created successfully.")

# --- Insert Data into Bronze Table ---
for _, row in df.iterrows():
    try:
        session.execute(
            """
            INSERT INTO bronze_table (order_id, sale_date, amount)
            VALUES (%s, %s, %s)
            """,
            [row['order_id'], row['sale_date'], row['amount']]
        )
    except Exception as e:
        print(f"Error inserting row into Bronze Table: {row} - {e}")

print("Data inserted into Bronze Table successfully.")

# --- Insert Data into Silver Table ---
for _, row in df.iterrows():
    try:
        session.execute(
            """
            INSERT INTO silver_table (sale_id, order_id, sale_date, amount)
            VALUES (uuid(), %s, %s, %s)
            """,
            [row['order_id'], row['sale_date'], row['amount']]
        )
    except Exception as e:
        print(f"Error inserting row into Silver Table: {row} - {e}")

print("Data inserted into Silver Table successfully.")

# --- Gold Table 1: Total Sales by Order ID ---
gold1 = df.groupby('order_id').agg({'amount': 'sum'}).reset_index()

# Ensure correct data types
gold1['order_id'] = gold1['order_id'].astype('int', errors='ignore')
gold1['amount'] = gold1['amount'].astype('float', errors='ignore')

for _, row in gold1.iterrows():
    try:
        session.execute(
            """
            INSERT INTO gold_table1 (order_id, total_sales)
            VALUES (%s, %s)
            """,
            [row['order_id'], row['amount']]
        )
    except Exception as e:
        print(f"Error inserting row into Gold Table 1: order_id={row['order_id']}, total_sales={row['amount']} - {e}")

print("Data inserted into Gold Table 1 successfully.")

# --- Gold Table 2: Total Sales by Sale Date ---
gold2 = df.groupby('sale_date').agg({'amount': 'sum'}).reset_index()

gold2['sale_date'] = pd.to_datetime(gold2['sale_date']).dt.date
gold2['amount'] = gold2['amount'].astype('float', errors='ignore')

for _, row in gold2.iterrows():
    try:
        session.execute(
            """
            INSERT INTO gold_table2 (sale_date, total_sales)
            VALUES (%s, %s)
            """,
            [row['sale_date'], row['amount']]
        )
    except Exception as e:
        print(f"Error inserting row into Gold Table 2: sale_date={row['sale_date']}, total_sales={row['amount']} - {e}")

print("Data inserted into Gold Table 2 successfully.")

# --- Gold Table 3: Total Sales by Region (if applicable) ---
if 'region' in df.columns:
    gold3 = df.groupby('region').agg({'amount': 'sum'}).reset_index()
    for _, row in gold3.iterrows():
        try:
            session.execute(
                """
                INSERT INTO gold_table3 (region, total_sales)
                VALUES (%s, %s)
                """,
                [row['region'], row['amount']]
            )
        except Exception as e:
            print(f"Error inserting row into Gold Table 3: region={row['region']}, total_sales={row['amount']} - {e}")
    print("Data inserted into Gold Table 3 successfully.")
else:
    print("Column 'region' does not exist in the dataset. Skipping Gold Table 3.")
