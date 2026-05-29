# Databricks notebook source
# MAGIC %md
# MAGIC # Lesson 2: Change Data Capture and Production Pipelines
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you will be able to:
# MAGIC - Implement Change Data Capture (CDC) using AUTO CDC INTO
# MAGIC - Understand SCD Type 1 slowly changing dimensions
# MAGIC - Handle INSERT, UPDATE, and DELETE operations automatically
# MAGIC - Add new data sources to an existing pipeline
# MAGIC - Schedule pipelines for production
# MAGIC - Apply production best practices
# MAGIC
# MAGIC ## Duration: ~60 minutes
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lesson 1: Building Pipelines with Data Quality
# MAGIC - Have your pipeline from Lesson 1 ready

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Change Data Capture (CDC)?
# MAGIC
# MAGIC **Change Data Capture** is a pattern for tracking changes to data over time. Instead of replacing entire tables, CDC captures individual changes (INSERT, UPDATE, DELETE) and applies them incrementally.
# MAGIC
# MAGIC ### Real-World Use Cases:
# MAGIC
# MAGIC - **Customer Master Data**: Track changes to customer information (address, email, status)
# MAGIC - **Product Catalog**: Maintain current product details, prices, availability
# MAGIC - **Employee Records**: Keep HR data current while optionally preserving history
# MAGIC - **Inventory Management**: Track real-time stock levels with changes
# MAGIC
# MAGIC ### Traditional Approach (Manual MERGE):
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO target_table
# MAGIC USING source_stream
# MAGIC ON target_table.id = source_stream.id
# MAGIC WHEN MATCHED AND source_stream.operation = 'UPDATE' 
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND source_stream.operation = 'DELETE' 
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND source_stream.operation = 'INSERT' 
# MAGIC   THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC **Problems:**
# MAGIC - Complex SQL logic
# MAGIC - Must handle out-of-order events manually
# MAGIC - Need to manage deduplication
# MAGIC - Performance tuning required
# MAGIC
# MAGIC ### Lakeflow AUTO CDC Approach:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FLOW customers_cdc AS 
# MAGIC AUTO CDC INTO target_table
# MAGIC FROM STREAM source_table
# MAGIC   KEYS (customer_id)
# MAGIC   SEQUENCE BY timestamp
# MAGIC   STORED AS SCD TYPE 1
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC - ✅ Declarative - just specify what you want
# MAGIC - ✅ Automatic ordering of events
# MAGIC - ✅ Built-in deduplication
# MAGIC - ✅ Optimized performance
# MAGIC - ✅ Handles late-arriving data

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Understanding SCD Type 1 vs Type 2
# MAGIC
# MAGIC ### SCD Type 1: Current State Only
# MAGIC
# MAGIC Overwrites old values with new values. Only the current state is maintained.
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC Initial Insert:
# MAGIC   customer_id='C001', name='John Doe', email='john@example.com'
# MAGIC
# MAGIC After Update:
# MAGIC   customer_id='C001', name='John Smith', email='john.smith@example.com'
# MAGIC   (old values are lost)
# MAGIC
# MAGIC After Delete:
# MAGIC   (row is removed from table)
# MAGIC ```
# MAGIC
# MAGIC **Use When:**
# MAGIC - History doesn't matter
# MAGIC - Storage/performance is critical
# MAGIC - Only current state is needed for analytics
# MAGIC - Example: Product catalog, current inventory levels
# MAGIC
# MAGIC ### SCD Type 2: Historical Tracking
# MAGIC
# MAGIC Preserves history by creating new rows for each change with start/end timestamps.
# MAGIC
# MAGIC **Example:**
# MAGIC ```
# MAGIC Initial Insert:
# MAGIC   customer_id='C001', name='John Doe', email='john@example.com'
# MAGIC   __START_AT='2024-01-01', __END_AT=NULL, __CURRENT=true
# MAGIC
# MAGIC After Update:
# MAGIC   Row 1 (old): __END_AT='2024-02-01', __CURRENT=false
# MAGIC   Row 2 (new): name='John Smith', email='john.smith@example.com'
# MAGIC                __START_AT='2024-02-01', __END_AT=NULL, __CURRENT=true
# MAGIC ```
# MAGIC
# MAGIC **Use When:**
# MAGIC - Audit trail is required
# MAGIC - Historical analysis is needed
# MAGIC - Compliance requires tracking changes
# MAGIC - Example: Employee salary history, regulatory data
# MAGIC
# MAGIC **In this lesson**: We'll use SCD Type 1 for simplicity

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Review the Customer CDC Pipeline Code
# MAGIC
# MAGIC Before adding it to your pipeline, let's understand the `customers_pipeline.sql` file.
# MAGIC
# MAGIC ### Step 1: Open the File
# MAGIC
# MAGIC 1. In your workspace, navigate to **2 - CDC and Production** folder
# MAGIC 2. Open **customers_pipeline.sql**
# MAGIC 3. Review the structure
# MAGIC
# MAGIC ### Key Components:
# MAGIC
# MAGIC #### 1. Bronze Layer - Raw CDC Events
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze.customers_raw
# MAGIC AS 
# MAGIC SELECT *, current_timestamp() AS processing_time
# MAGIC FROM STREAM read_files("${source}/customers", format => 'json');
# MAGIC ```
# MAGIC
# MAGIC **What it does:**
# MAGIC - Ingests CDC events from JSON files
# MAGIC - Each event contains: data fields + `operation` type + `timestamp`
# MAGIC - Operations: INSERT, UPDATE, DELETE
# MAGIC
# MAGIC #### 2. Bronze Layer - Data Quality Validation
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze.customers_clean
# MAGIC   (
# MAGIC     CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC     CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC     CONSTRAINT valid_email EXPECT (rlike(email, '...') OR operation = 'DELETE')
# MAGIC   )
# MAGIC AS SELECT *, CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
# MAGIC FROM STREAM bronze.customers_raw;
# MAGIC ```
# MAGIC
# MAGIC **Critical:** Always validate CDC data BEFORE applying changes!
# MAGIC - Invalid data can corrupt your target table
# MAGIC - Use FAIL UPDATE for critical constraints
# MAGIC - Allow nulls for DELETE operations
# MAGIC
# MAGIC #### 3. Silver Layer - Target Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver.customers;
# MAGIC ```
# MAGIC
# MAGIC **Simple definition** - AUTO CDC will manage the contents!
# MAGIC
# MAGIC #### 4. CREATE FLOW - The CDC Magic
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FLOW customers_cdc AS 
# MAGIC AUTO CDC INTO silver.customers
# MAGIC FROM STREAM bronze.customers_clean
# MAGIC   KEYS (customer_id)                    -- Primary key for matching
# MAGIC   APPLY AS DELETE WHEN operation = 'DELETE'  -- Handle deletes
# MAGIC   SEQUENCE BY timestamp_datetime        -- Order of operations
# MAGIC   COLUMNS * EXCEPT (timestamp, operation, ...)  -- Exclude metadata
# MAGIC   STORED AS SCD TYPE 1;                 -- Current state only
# MAGIC ```
# MAGIC
# MAGIC **Let's break down each clause:**

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Understanding AUTO CDC Clauses
# MAGIC
# MAGIC ### KEYS (customer_id)
# MAGIC
# MAGIC **Purpose:** Defines the primary key(s) for matching records
# MAGIC
# MAGIC **How it works:**
# MAGIC - For INSERT: Checks if key exists, inserts if new
# MAGIC - For UPDATE: Finds existing row by key, updates values
# MAGIC - For DELETE: Finds existing row by key, removes it
# MAGIC
# MAGIC **Examples:**
# MAGIC ```sql
# MAGIC KEYS (customer_id)                    -- Single key
# MAGIC KEYS (order_id, line_number)         -- Composite key
# MAGIC KEYS (store_id, product_id, date)    -- Multi-column key
# MAGIC ```
# MAGIC
# MAGIC ### SEQUENCE BY timestamp_datetime
# MAGIC
# MAGIC **Purpose:** Determines the order of applying changes
# MAGIC
# MAGIC **Why it matters:**
# MAGIC - Events may arrive out of order
# MAGIC - Later updates shouldn't be overwritten by older updates
# MAGIC - Deletes should be applied last
# MAGIC
# MAGIC **Example scenario:**
# MAGIC ```
# MAGIC Events arrive in this order:
# MAGIC   1. DELETE (timestamp: 3:00 PM)
# MAGIC   2. UPDATE (timestamp: 2:00 PM)  
# MAGIC   3. INSERT (timestamp: 1:00 PM)
# MAGIC
# MAGIC AUTO CDC processes in correct order:
# MAGIC   1. INSERT (1:00 PM)
# MAGIC   2. UPDATE (2:00 PM)
# MAGIC   3. DELETE (3:00 PM)
# MAGIC ```
# MAGIC
# MAGIC ### APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC
# MAGIC **Purpose:** Specifies which records trigger deletions
# MAGIC
# MAGIC **Why needed:**
# MAGIC - Source system sends "operation='DELETE'" for removed records
# MAGIC - AUTO CDC needs to know which records to remove from target
# MAGIC
# MAGIC **Can use complex logic:**
# MAGIC ```sql
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE' OR status = 'INACTIVE'
# MAGIC ```
# MAGIC
# MAGIC ### COLUMNS * EXCEPT (timestamp, operation, ...)
# MAGIC
# MAGIC **Purpose:** Selects which columns to include in target table
# MAGIC
# MAGIC **Why use EXCEPT:**
# MAGIC - Target table should only have business data
# MAGIC - Exclude CDC metadata (operation, processing_time, etc.)
# MAGIC - Exclude technical fields from source
# MAGIC
# MAGIC ### STORED AS SCD TYPE 1
# MAGIC
# MAGIC **Purpose:** Defines how history is handled
# MAGIC
# MAGIC **Options:**
# MAGIC - `SCD TYPE 1`: Current state only (overwrites)
# MAGIC - `SCD TYPE 2`: Maintains history with start/end dates

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Add Customer Pipeline to Your Existing Pipeline
# MAGIC
# MAGIC Now let's add CDC capabilities to your orders pipeline!
# MAGIC
# MAGIC ### Step 1: Move the File
# MAGIC
# MAGIC 1. In your workspace file browser, locate **Exercise_2/customers_pipeline.sql**
# MAGIC 2. **Drag and drop** (or cut/paste) the file to **transformations/** folder
# MAGIC 3. Confirm the file is now in the transformations folder
# MAGIC
# MAGIC ### Step 2: Open Your Pipeline
# MAGIC
# MAGIC 1. Return to your pipeline from Lesson 1
# MAGIC 2. Look at the **Pipeline** tab in the left sidebar
# MAGIC 3. You should now see **two files**:
# MAGIC    - orders_pipeline.sql
# MAGIC    - customers_pipeline.sql

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Run the Pipeline with CDC
# MAGIC
# MAGIC Let's see AUTO CDC in action!
# MAGIC
# MAGIC ### Step 1: Start the Pipeline
# MAGIC
# MAGIC 1. Click **Run pipeline** in the toolbar
# MAGIC 2. Watch the pipeline graph as it executes
# MAGIC 3. Both orders and customers will process
# MAGIC
# MAGIC ### Step 2: Observe CDC Processing
# MAGIC
# MAGIC **In the Pipeline Graph:**
# MAGIC
# MAGIC 1. **bronze.customers_raw**: Will show 27 records (20 INSERTs + 5 UPDATEs + 2 DELETEs)
# MAGIC 2. **bronze.customers_clean**: Should show 27 records (all validated)
# MAGIC 3. **silver.customers**: Will show **18 records** (current state)
# MAGIC    - Started with 20 inserts
# MAGIC    - 5 were updated (same count, values changed)
# MAGIC    - 2 were deleted (removed)
# MAGIC    - Final: 20 - 2 = 18 customers
# MAGIC
# MAGIC ### Step 3: Verify the Results
# MAGIC
# MAGIC Click on the **silver.customers** node and review the data:
# MAGIC - Customer 1: Should have **updated** email and address
# MAGIC - Customer 3: Should be **missing** (deleted)
# MAGIC - Customer 10: Should have **updated** information
# MAGIC
# MAGIC ### Step 4: Check Data Quality Metrics
# MAGIC
# MAGIC 1. Click **bronze.customers_clean**
# MAGIC 2. View the **Table metrcis** tab
# MAGIC 3. Verify all constraints passed:
# MAGIC    - valid_id: 27 met
# MAGIC    - valid_operation: 27 met
# MAGIC    - valid_email: 25 met (DELETEs don't need emails)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Query the CDC Results
# MAGIC
# MAGIC Let's verify the CDC operations worked correctly.

# COMMAND ----------

# DBTITLE 1,Set catalog and schema
# MAGIC %py
# MAGIC # Set catalog for SQL queries in this notebook
# MAGIC import re
# MAGIC current_user = spark.sql("SELECT current_user()").collect()[0][0]
# MAGIC username = current_user.split("@")[0]
# MAGIC clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())
# MAGIC catalog_name = f"sdp_workshop_{clean_username}"
# MAGIC
# MAGIC # Set as default catalog for all queries
# MAGIC spark.sql(f"USE CATALOG {catalog_name}")
# MAGIC print(f"✓ Using catalog: {catalog_name}")
# MAGIC print("  All SQL queries will use this catalog automatically")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the final current state
# MAGIC SELECT * FROM silver.customers ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify record counts
# MAGIC SELECT 
# MAGIC   'customers_raw' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM bronze.customers_raw
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'customers_clean' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM bronze.customers_clean
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'customers (current)' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check which customers were updated (have SF address)
# MAGIC SELECT customer_id, name, email, city, state
# MAGIC FROM silver.customers
# MAGIC WHERE city = 'San Francisco'
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deleted customers are gone
# MAGIC SELECT customer_id 
# MAGIC FROM bronze.customers_raw
# MAGIC WHERE operation = 'DELETE'
# MAGIC EXCEPT
# MAGIC SELECT customer_id
# MAGIC FROM silver.customers;
# MAGIC -- Should return the deleted customer IDs (CUST0003, CUST0007)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Schedule the Pipeline for Production
# MAGIC
# MAGIC Now that our pipeline handles both orders and customer CDC, let's schedule it for production!
# MAGIC
# MAGIC ### Step 1: Schedule a Pipeline with Lakeflow jobs
# MAGIC
# MAGIC 1. Select the **Schedule** icon (🗓️) in the top left of the **Pipeline** tab OR on the top right of the page (in between **Settings** and **Share**)
# MAGIC 2. Select **Add Schedule**
# MAGIC 3. Job Name: `SDP Workshop - {user_name}`
# MAGIC 4. Select **Simple**
# MAGIC 5. Select Schedule Every `1` `Day`
# MAGIC 5. Keep `Performance Optimized` checked. Un-checking this will run the pipeline in Serverless Standard Mode (better TCO but slower spin up time)
# MAGIC 6. Select **Create**
# MAGIC 7. Select the hyperlink to your new job
# MAGIC
# MAGIC You will notice you are now in the Lakeflow Jobs canvas. All SDP pipelines are orchestrated with Lakeflow Jobs. This means they can be combined with other task types and take advantage of all Jobs production orchestration features like notifications, retries, and metric thresholds.
# MAGIC
# MAGIC ## Step 2: Run and monitor the Pipeline
# MAGIC
# MAGIC 1. Select **Run now**
# MAGIC 2. You can now monitor the Job run in Runs tab
# MAGIC 3. If you navigate to **Jobs & Pipelines** > Runs you will see that scheduled pipelines show up in a unified list with all other Lakeflow Jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Key Takeaways - Lesson 2
# MAGIC
# MAGIC ✅ **AUTO CDC INTO** simplifies Change Data Capture with declarative syntax
# MAGIC
# MAGIC ✅ **SCD Type 1** maintains current state, Type 2 preserves history
# MAGIC
# MAGIC ✅ **KEYS** clause defines primary key for matching records
# MAGIC
# MAGIC ✅ **SEQUENCE BY** ensures correct ordering of out-of-order events
# MAGIC
# MAGIC ✅ **APPLY AS DELETE WHEN** specifies deletion condition
# MAGIC
# MAGIC ✅ **Multi-file pipelines** automatically discover and orchestrate dependencies
# MAGIC
# MAGIC ✅ **Production scheduling** enables automated, reliable data processing
# MAGIC
# MAGIC ✅ **Data quality expectations** should be validated BEFORE applying CDC
# MAGIC
# MAGIC ✅ **Monitoring and alerting** are critical for production pipelines
# MAGIC
# MAGIC ## You've Built a Production Pipeline!
# MAGIC
# MAGIC You now have a scheduled production pipeline that:
# MAGIC - Ingests order data incrementally with Auto Loader
# MAGIC - Applies Change Data Capture for customer updates
# MAGIC - Enforces data quality at every layer
# MAGIC - Runs automatically on a schedule
# MAGIC - Follows production best practices

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Workshop
# MAGIC
# MAGIC Congratulations! You've completed the Lakeflow Spark Declarative Pipelines workshop!
# MAGIC
# MAGIC ### What You've Learned:
# MAGIC
# MAGIC 1. ✅ Building declarative pipelines with medallion architecture
# MAGIC 2. ✅ Implementing data quality with expectations
# MAGIC 3. ✅ Using Auto Loader for incremental file ingestion
# MAGIC 4. ✅ Applying Change Data Capture with AUTO CDC INTO
# MAGIC 5. ✅ Managing multi-file pipeline projects
# MAGIC 6. ✅ Scheduling pipelines for production
# MAGIC 7. ✅ Production best practices
# MAGIC
# MAGIC Thank you for participating! 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Cleanup Workshop Resources
# MAGIC
# MAGIC **IMPORTANT:** Only run this cell if you want to completely remove all workshop resources.
# MAGIC
# MAGIC This will delete:
# MAGIC - Your catalog (sdp_workshop_<username>)
# MAGIC - All schemas (bronze, silver, gold)
# MAGIC - All tables and data
# MAGIC - The raw volume and all source files
# MAGIC - UC functions (add_orders, add_status)
# MAGIC
# MAGIC **This action cannot be undone!**

# COMMAND ----------

# Uncomment the lines below to cleanup all workshop resources

# import re
# current_user = spark.sql("SELECT current_user()").collect()[0][0]
# username = current_user.split("@")[0]
# clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())
# catalog_name = f"sdp_workshop_{clean_username}"
#
# print(f"WARNING: About to delete catalog: {catalog_name}")
# print("This will remove ALL workshop data, tables, and volumes!")
# print("\nUncomment the DROP CATALOG line below to proceed...")
#
# # Uncomment this line to actually delete:
# # spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
# # print(f"✓ Deleted catalog: {catalog_name}")
# # print("✓ Workshop cleanup complete!")