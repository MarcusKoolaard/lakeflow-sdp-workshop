# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Setup - Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC This notebook sets up the environment for the 90-minute Lakeflow Spark Declarative Pipelines workshop.
# MAGIC
# MAGIC **Run this notebook ONCE at the beginning of the workshop.**
# MAGIC
# MAGIC ## What This Setup Creates:
# MAGIC
# MAGIC 1. **Catalog** - User-specific catalog (sdp_workshop_<username>)
# MAGIC 2. **Schemas** - Bronze, Silver, and Gold schemas for medallion architecture
# MAGIC 3. **Raw Volume** - A UC Volume for landing raw source data files
# MAGIC 4. **Sample Data** - Initial JSON files for orders, status, and customers
# MAGIC
# MAGIC ## Workshop Structure:
# MAGIC
# MAGIC - **Exercise 1** (40 min): Build a simple pipeline with orders data
# MAGIC - **Exercise 2** (50 min): Add customer CDC and schedule for production

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Workshop Environment

# COMMAND ----------

import re

# Get current user information
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0]

# Clean username for use in naming (remove special characters)
clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())

# Create a helper class
class WorkshopHelper:
    def __init__(self):
        self.username = username
        self.clean_username = clean_username
        self.catalog_name = f"sdp_workshop_{clean_username}"  # Catalog with username
        self.default_schema = "default"  # For volume storage
        
        # Define paths
        self.working_dir = f"/Volumes/{self.catalog_name}/{self.default_schema}/raw"
        
        # Schema names - simple medallion architecture
        self.bronze_schema = "bronze"
        self.silver_schema = "silver"
        self.gold_schema = "gold"
    
    def print_config(self):
        print(f"""
Workshop Configuration
=====================
User: {self.username}
Catalog: {self.catalog_name}
Working Directory: {self.working_dir}

Schemas:
- Bronze: {self.catalog_name}.{self.bronze_schema}
- Silver: {self.catalog_name}.{self.silver_schema}
- Gold: {self.catalog_name}.{self.gold_schema}
        """)

# Initialize the helper
DA = WorkshopHelper()
DA.print_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Cleanup Previous Workshop Resources (if any)

# COMMAND ----------

# Drop existing catalog if it exists (cascades to all schemas, tables, volumes)
try:
    spark.sql(f"DROP CATALOG IF EXISTS {DA.catalog_name} CASCADE")
    print(f"✓ Cleaned up existing catalog: {DA.catalog_name}")
except Exception as e:
    print(f"Note: No previous catalog to clean up (this is normal for first run)")

print(f"\nStarting fresh setup for: {DA.catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Catalog and Schemas

# COMMAND ----------

# Create the catalog first
spark.sql(f"CREATE CATALOG IF NOT EXISTS {DA.catalog_name}")
print(f"✓ Created catalog: {DA.catalog_name}")

# Create the default schema for volumes
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{DA.default_schema}")
print(f"✓ Created schema: {DA.catalog_name}.{DA.default_schema}")

# Create the three schemas for medallion architecture
schemas_to_create = [DA.bronze_schema, DA.silver_schema, DA.gold_schema]

for schema in schemas_to_create:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{schema}")
    print(f"✓ Created schema: {DA.catalog_name}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Raw Volume for Source Data

# COMMAND ----------

# Create volume in the default schema for raw source files
volume_name = "raw"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {DA.catalog_name}.{DA.default_schema}.{volume_name}")
print(f"✓ Created volume: {DA.catalog_name}.{DA.default_schema}.{volume_name}")
print(f"  Path: {DA.working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Raw Source Data Directories

# COMMAND ----------

# Create directories for source data
dbutils.fs.mkdirs(f"{DA.working_dir}/orders")
dbutils.fs.mkdirs(f"{DA.working_dir}/status")
dbutils.fs.mkdirs(f"{DA.working_dir}/customers")

print("✓ Created raw source data directories:")
print(f"  - {DA.working_dir}/orders")
print(f"  - {DA.working_dir}/status")
print(f"  - {DA.working_dir}/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Generate Sample Orders Data

# COMMAND ----------

import json
from datetime import datetime, timedelta
import random

# Generate sample orders
def generate_orders(num_orders=174, file_name="00.json"):
    """Generate sample orders data"""
    orders = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(num_orders):
        order = {
            "order_id": f"ORD{i+1000:05d}",
            "order_timestamp": (base_date + timedelta(days=random.randint(0, 30))).isoformat(),
            "customer_id": f"CUST{random.randint(1, 100):04d}",
            "notifications": {
                "email": random.choice([True, False]),
                "sms": random.choice([True, False])
            }
        }
        orders.append(order)
    
    # Write to volume
    file_path = f"{DA.working_dir}/orders/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(order) for order in orders]), overwrite=True)
    
    return len(orders)

# Generate initial orders file
num_orders = generate_orders(num_orders=174, file_name="00.json")
print(f"✓ Generated {num_orders} sample orders in 00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Generate Sample Status Data

# COMMAND ----------

def generate_status_updates(num_updates=536, file_name="00.json"):
    """Generate sample order status updates"""
    statuses = ['placed', 'preparing', 'on the way', 'delivered', 'canceled']
    status_updates = []
    
    base_timestamp = datetime(2024, 1, 1).timestamp()
    
    for i in range(num_updates):
        update = {
            "order_id": f"ORD{random.randint(1000, 1173):05d}",
            "order_status": random.choice(statuses),
            "status_timestamp": base_timestamp + (i * 3600)  # Unix timestamp
        }
        status_updates.append(update)
    
    # Write to volume
    file_path = f"{DA.working_dir}/status/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(update) for update in status_updates]), overwrite=True)
    
    return len(status_updates)

# Generate initial status file
num_status = generate_status_updates(num_updates=536, file_name="00.json")
print(f"✓ Generated {num_status} sample status updates in 00.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Generate Sample Customer CDC Data

# COMMAND ----------

def generate_customer_cdc(file_name="00.json"):
    """Generate sample customer CDC events"""
    customers = []
    base_timestamp = datetime(2024, 1, 1).timestamp()
    
    # INSERT operations - 20 new customers
    for i in range(1, 21):
        customer = {
            "customer_id": f"CUST{i:04d}",
            "name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "address": f"{i*100} Main St",
            "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston"]),
            "state": random.choice(["NY", "CA", "IL", "TX"]),
            "zip_code": f"{10000 + i:05d}",
            "operation": "INSERT",
            "timestamp": base_timestamp + (i * 1000)
        }
        customers.append(customer)
    
    # UPDATE operations - 5 customers change email/address
    for i in [1, 5, 10, 15, 20]:
        customer = {
            "customer_id": f"CUST{i:04d}",
            "name": f"Customer {i}",
            "email": f"newemail{i}@example.com",  # Email changed
            "address": f"{i*200} Oak Ave",  # Address changed
            "city": "San Francisco",  # City changed
            "state": "CA",
            "zip_code": f"{94000 + i:05d}",
            "operation": "UPDATE",
            "timestamp": base_timestamp + (30 * 1000) + (i * 100)  # Later timestamps
        }
        customers.append(customer)
    
    # DELETE operations - 2 customers removed
    for i in [3, 7]:
        customer = {
            "customer_id": f"CUST{i:04d}",
            "operation": "DELETE",
            "timestamp": base_timestamp + (60 * 1000) + (i * 100)  # Even later
        }
        customers.append(customer)
    
    # Write to volume
    file_path = f"{DA.working_dir}/customers/{file_name}"
    dbutils.fs.put(file_path, "\n".join([json.dumps(c) for c in customers]), overwrite=True)
    
    return len(customers)

# Generate initial customer CDC file
num_customers = generate_customer_cdc(file_name="00.json")
print(f"✓ Generated {num_customers} customer CDC events in 00.json")
print(f"  - 20 INSERT operations")
print(f"  - 5 UPDATE operations")
print(f"  - 2 DELETE operations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Setup Complete!

# COMMAND ----------

catalog = DA.catalog_name
working_dir = DA.working_dir

print(f"""
================================================================================
                    WORKSHOP SETUP COMPLETE! ✓
================================================================================

IMPORTANT: Save these values for your pipeline settings:

1. Default Catalog: {catalog}
2. Default Schema: bronze
3. Configuration Variable:
     Key: source
     Value: {working_dir}

Raw data landing zone:
  {working_dir}

Schemas created:
  • {catalog}.bronze
  • {catalog}.silver
  • {catalog}.gold

Sample raw data created:
  • 174 orders in orders/00.json
  • 536 status updates in status/00.json
  • 27 customer CDC events in customers/00.json

--------------------------------------------------------------------------------
Next Steps:
  1. Open "Exercise_1/1-Building_Pipelines_with_Data_Quality.sql"
  2. Create your first pipeline
================================================================================
""")