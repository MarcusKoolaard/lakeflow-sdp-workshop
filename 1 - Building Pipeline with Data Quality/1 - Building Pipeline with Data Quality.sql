-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Lesson 1: Building Data Pipelines with Data Quality
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you will be able to:
-- MAGIC - Create a Lakeflow Spark Declarative Pipeline using the multi-file editor
-- MAGIC - Implement medallion architecture (Bronze → Silver → Gold)
-- MAGIC - Apply data quality expectations with proper violation handling
-- MAGIC - Process data incrementally with Auto Loader
-- MAGIC - Monitor pipeline execution and view results
-- MAGIC
-- MAGIC ## Duration: ~50 minutes
-- MAGIC
-- MAGIC ## Prerequisites
-- MAGIC - Completed running **0-SETUP.py** notebook

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## What is Lakeflow Spark Declarative Pipelines?
-- MAGIC
-- MAGIC **Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables) is a declarative framework for building reliable, maintainable, and testable data pipelines.
-- MAGIC
-- MAGIC ### Key Features:
-- MAGIC
-- MAGIC - **Declarative**: Define *what* you want, not *how* to do it
-- MAGIC - **Automatic Dependency Management**: The system figures out execution order
-- MAGIC - **Built-in Data Quality**: Expectations enforce data quality at ingestion
-- MAGIC - **Incremental Processing**: Only process new data automatically
-- MAGIC - **Multi-File Organization**: Organize code logically across files
-- MAGIC - **Integrated IDE**: Complete development environment with DAG, previews, monitoring
-- MAGIC
-- MAGIC ### The Lakeflow Pipelines Editor:
-- MAGIC
-- MAGIC Unlike traditional notebooks, the new **Lakeflow Pipelines Editor** provides:
-- MAGIC - Multi-file editing with tabs
-- MAGIC - Live pipeline graph (DAG)
-- MAGIC - Inline data previews
-- MAGIC - Integrated performance monitoring
-- MAGIC - Selective execution (run file, run table, run pipeline)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Understanding the Pipeline Architecture
-- MAGIC
-- MAGIC In this lesson, we'll build a simple pipeline following the **medallion architecture**:
-- MAGIC
-- MAGIC ```
-- MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
-- MAGIC │   BRONZE    │     │   SILVER    │     │    GOLD     │
-- MAGIC │             │────▶│             │────▶│             │
-- MAGIC │  Raw Data   │     │ Clean Data  │     │ Aggregated  │
-- MAGIC └─────────────┘     └─────────────┘     └─────────────┘
-- MAGIC ```
-- MAGIC
-- MAGIC ### Our Pipeline:
-- MAGIC
-- MAGIC 1. **Bronze**: `bronze.orders`
-- MAGIC    - Ingest raw JSON files from cloud storage
-- MAGIC    - Preserve all source data
-- MAGIC    - Add metadata (processing time, source file)
-- MAGIC
-- MAGIC 2. **Silver**: `silver.orders_clean`
-- MAGIC    - Parse and validate data types
-- MAGIC    - Enforce data quality expectations
-- MAGIC    - Select relevant columns
-- MAGIC
-- MAGIC 3. **Gold**: `gold.order_summary`
-- MAGIC    - Business-level aggregations
-- MAGIC    - Daily order summaries
-- MAGIC    - Ready for analytics/reporting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Create Your First Pipeline
-- MAGIC
-- MAGIC Let's create a pipeline using the Lakeflow Pipelines Editor.
-- MAGIC
-- MAGIC ### Step 1: Open the Pipeline Creator
-- MAGIC
-- MAGIC 1. In the **left sidebar**, click **New** (the blue button at top)
-- MAGIC 2. Select **ETL pipeline**
-- MAGIC
-- MAGIC ### Step 2: Configure Basic Settings
-- MAGIC
-- MAGIC 1. **Pipeline name**: `SDP Workshop - [your-username]`
-- MAGIC    - Example: `SDP Workshop - john_doe`
-- MAGIC
-- MAGIC 2. **Default catalog**: Your catalog name
-- MAGIC    - This was created by the setup notebook
-- MAGIC    - This will be in the format `sdp_workshop_john_doe`
-- MAGIC
-- MAGIC 3. **Default schema**: `bronze`
-- MAGIC
-- MAGIC 4. **Creation option**: Select **"Add existing assets"**
-- MAGIC    - Pipeline root folder: select the folder you just imported **Build Data Pipelines with Lakeflow Spark Declarative Pipeline**
-- MAGIC    - Source code paths: select **transformations**
-- MAGIC
-- MAGIC 5. Click **Add**
-- MAGIC
-- MAGIC You should now see the **Lakeflow Pipelines Editor** with:
-- MAGIC - Pipeline assets browser on the left
-- MAGIC - Code editor in the center
-- MAGIC - Empty pipeline graph on the right

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Explore the Pipeline Editor Interface
-- MAGIC
-- MAGIC Before we add code, let's understand the interface:
-- MAGIC
-- MAGIC ### Left Panel: Pipeline Assets Browser
-- MAGIC
-- MAGIC - **Pipeline tab**: Shows files in your pipeline project
-- MAGIC - **All files tab**: Access to your entire workspace
-- MAGIC - **Settings icon** (⚙️): Pipeline configuration
-- MAGIC - **Schedule icon** (📅): Set up automated runs
-- MAGIC - **Share icon** (👥): Manage permissions
-- MAGIC
-- MAGIC ### Center Panel: Code Editor
-- MAGIC
-- MAGIC - Tabbed interface for multiple files
-- MAGIC - Syntax highlighting for SQL and Python
-- MAGIC - Run controls in toolbar
-- MAGIC
-- MAGIC ### Right Panel: Pipeline Graph (DAG)
-- MAGIC
-- MAGIC - Visual representation of data flow
-- MAGIC - Updates in real-time during execution
-- MAGIC - Click nodes to see data preview
-- MAGIC
-- MAGIC ### Bottom Panel: Details & Monitoring
-- MAGIC
-- MAGIC - **Tables**: List of all tables with metrics
-- MAGIC - **Performance**: Execution profiles
-- MAGIC - **Issues**: Errors and warnings
-- MAGIC - **Event Log**: Detailed execution events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Configure Pipeline Settings
-- MAGIC
-- MAGIC Before adding code, we need to configure the pipeline:
-- MAGIC
-- MAGIC ### Step 1: Open Settings
-- MAGIC
-- MAGIC 1. Click the **Settings** icon (⚙️) in the left sidebar
-- MAGIC 2. The settings panel opens on the right
-- MAGIC
-- MAGIC ### Step 2: Review General Settings
-- MAGIC
-- MAGIC - **Pipeline name**: Should match what you entered
-- MAGIC - **Pipeline mode**: Should be **Triggered** (default)
-- MAGIC - **Run as**: Your user account (can't change without permissions)
-- MAGIC
-- MAGIC ### Step 3: Verify Code Assets
-- MAGIC
-- MAGIC - **Root folder**: Your pipeline project folder
-- MAGIC - **Source code**: Should be empty or show the default file
-- MAGIC
-- MAGIC ### Step 4: Confirm Default Location
-- MAGIC
-- MAGIC - **Default catalog**: Should be your catalog
-- MAGIC - **Default schema**: Should be `bronze`
-- MAGIC - These determine where tables are created if not explicitly specified
-- MAGIC
-- MAGIC ### Step 5: Configure Compute
-- MAGIC
-- MAGIC 1. Look at the **Compute** section
-- MAGIC 2. Ensure **Serverless** is selected (recommended)
-- MAGIC 3. If not available, classic compute will work but takes longer to start
-- MAGIC
-- MAGIC ### Step 6: Add Configuration Variable ⚠️ IMPORTANT
-- MAGIC
-- MAGIC This is critical - the SQL code references a `${source}` variable:
-- MAGIC
-- MAGIC 1. In the **Configuration** section, click **Add configuration**
-- MAGIC 2. **Key**: `source`
-- MAGIC 3. **Value**: Your volume path from the setup
-- MAGIC    - Format: `/Volumes/{your-catalog}/default/raw`
-- MAGIC    - Example: `/Volumes/sdp_workshop_john_doe/default/raw`
-- MAGIC    - If you don't remember: Go back to the 0-SETUP output to see it
-- MAGIC 4. Click **Save**
-- MAGIC
-- MAGIC ### Step 7: Save Settings
-- MAGIC
-- MAGIC - Close the settings panel by clicking anywhere in the code editor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## E. Understanding the Orders Pipeline Code
-- MAGIC
-- MAGIC Navigate to the pipeline tab > transformations and open the `orders_pipeline.sql` file
-- MAGIC
-- MAGIC Let's break down what the code does:
-- MAGIC
-- MAGIC ### Bronze Layer - Raw Ingestion
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE OR REFRESH STREAMING TABLE bronze.orders
-- MAGIC   COMMENT "Raw orders data ingested from JSON files"
-- MAGIC   TBLPROPERTIES ("pipelines.reset.allowed" = false)
-- MAGIC AS 
-- MAGIC SELECT *, current_timestamp() AS processing_time, ...
-- MAGIC FROM STREAM read_files("${source}/orders", format => 'json');
-- MAGIC ```
-- MAGIC
-- MAGIC **Key Concepts**:
-- MAGIC - `CREATE OR REFRESH STREAMING TABLE`: Defines an incrementally updated table
-- MAGIC - `STREAM read_files()`: Auto Loader - incrementally processes new files
-- MAGIC - `${source}`: Variable substitution from pipeline configuration
-- MAGIC - `pipelines.reset.allowed = false`: Prevents accidental full refresh
-- MAGIC - Checkpoint is managed automatically
-- MAGIC - Inherit default catalog and schema names or write out to different catalogs and schemas using the fully qualified name 
-- MAGIC
-- MAGIC ### Silver Layer - Clean Data
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE OR REFRESH STREAMING TABLE silver.orders_clean
-- MAGIC   (
-- MAGIC     CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
-- MAGIC     CONSTRAINT valid_timestamp EXPECT (order_timestamp > "2020-01-01")
-- MAGIC   )
-- MAGIC AS SELECT ... FROM STREAM bronze.orders;
-- MAGIC ```
-- MAGIC
-- MAGIC **Key Concepts**:
-- MAGIC - **Expectations**: Data quality rules
-- MAGIC - `ON VIOLATION FAIL UPDATE`: Stops pipeline if constraint fails
-- MAGIC - `FROM STREAM`: Reads incrementally from bronze table
-- MAGIC
-- MAGIC ### Gold Layer - Business Logic
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold.orders_summary
-- MAGIC AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
-- MAGIC FROM silver.orders_clean
-- MAGIC GROUP BY date(order_timestamp);
-- MAGIC ```
-- MAGIC
-- MAGIC **Key Concepts**:
-- MAGIC - `MATERIALIZED VIEW`: Persisted query results
-- MAGIC - No `STREAM` keyword: Reads all data from source
-- MAGIC - Automatically optimized incremental refresh when possible
-- MAGIC - Ideal for aggregations and analytics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Validate the Pipeline (Dry Run)
-- MAGIC
-- MAGIC Before running the pipeline, let's validate it:
-- MAGIC
-- MAGIC ### Step 1: Run a Dry Run
-- MAGIC
-- MAGIC 1. In the **toolbar** at the top, click **Dry run**
-- MAGIC 2. Wait 1-2 minutes for validation to complete
-- MAGIC 3. Watch the left panel - it will show "DRY RUN" mode
-- MAGIC
-- MAGIC ### Step 2: Review the Results
-- MAGIC
-- MAGIC After the dry run completes:
-- MAGIC
-- MAGIC 1. **Pipeline Graph** (right panel):
-- MAGIC    - Should show 3 nodes: `orders` → `orders_clean` → `orders_summary`
-- MAGIC    - Arrows show data flow
-- MAGIC    - No errors should appear
-- MAGIC
-- MAGIC 2. **Tables Panel** (bottom):
-- MAGIC    - Lists all 3 tables
-- MAGIC    - Shows catalog, schema, and type
-- MAGIC    - Status should be "Validated"
-- MAGIC
-- MAGIC 3. **Issues Panel** (bottom):
-- MAGIC    - Should be empty (no errors)
-- MAGIC    - If errors appear, check your configuration
-- MAGIC
-- MAGIC ### Troubleshooting Dry Run Errors:
-- MAGIC
-- MAGIC **Error: "Variable 'source' not found"**
-- MAGIC - Go back to Settings → Configuration
-- MAGIC - Verify the `source` variable is set correctly
-- MAGIC
-- MAGIC **Error: "Schema not found"**
-- MAGIC - Check that 0-SETUP.py ran successfully
-- MAGIC - Verify Default Catalog in Settings
-- MAGIC
-- MAGIC **Error: "Permission denied"**
-- MAGIC - Ensure you have CREATE privileges in the catalog
-- MAGIC - Contact your workspace admin if needed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Run one table at a time
-- MAGIC
-- MAGIC The new IDE for data engineering makes it super easy to iteratively build and test your code. Rather than running the whole pipeline, you can now just run one table at a time.
-- MAGIC
-- MAGIC 1. Click on the `dataset actions`(▶️) button to the top left of `CREATE OR REFRESH STREAMING TABLE bronze.orders`
-- MAGIC 2. Select **Run table** `sdp_workshop_{your_name}.bronze.orders`
-- MAGIC 2. In the Pipeline Graph you will see just the Orders Streaming Table executed
-- MAGIC
-- MAGIC After completion, you should see:
-- MAGIC
-- MAGIC - **orders** (bronze): 174 rows
-- MAGIC   - One JSON file (00.json) was processed
-- MAGIC   - All raw data preserved

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Run the Pipeline
-- MAGIC
-- MAGIC Now let's execute the full pipeline!
-- MAGIC
-- MAGIC ### Step 1: Start the Pipeline
-- MAGIC
-- MAGIC 1. In the **toolbar**, click **Run pipeline**
-- MAGIC 2. The pipeline will start executing
-- MAGIC 3. You'll see the status change in the left panel
-- MAGIC
-- MAGIC ### Step 2: Watch the Execution (2-3 minutes)
-- MAGIC
-- MAGIC Observe the **Pipeline Graph** as it runs:
-- MAGIC
-- MAGIC 1. **Starting**: Nodes appear gray/blue
-- MAGIC 2. **Running**: Nodes turn yellow with a spinner
-- MAGIC 3. **Complete**: Nodes turn green with checkmarks
-- MAGIC 4. **Row counts**: Numbers appear on edges showing data flow
-- MAGIC
-- MAGIC ### Step 3: Expected Results (First Run)
-- MAGIC
-- MAGIC After completion, you will now also see
-- MAGIC
-- MAGIC - **orders_clean** (silver): 174 rows
-- MAGIC   - Same count (all data passed validation)
-- MAGIC   - Data quality constraints were met
-- MAGIC
-- MAGIC - **order_summary** (gold): ~30 rows
-- MAGIC   - Aggregated by date
-- MAGIC   - ~30 unique dates in the sample data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## I. Explore the Pipeline Results
-- MAGIC
-- MAGIC Let's examine what the pipeline created:
-- MAGIC
-- MAGIC ### Step 1: Examine Data Quality
-- MAGIC
-- MAGIC 1. Click the **orders_clean** node
-- MAGIC 2. Look for the **Expectations** section in the table details
-- MAGIC 3. You should see:
-- MAGIC    - **valid_order_id**: 174 met (0 violations)
-- MAGIC    - **valid_timestamp**: 174 met (0 violations)
-- MAGIC    - **valid_customer_id**: 174 met (0 violations)
-- MAGIC
-- MAGIC ### Step 2: View Aggregated Data
-- MAGIC
-- MAGIC 1. Click the **order_summary** node
-- MAGIC 2. In the **Data** tab, you'll see:
-- MAGIC    - `order_date`: The date
-- MAGIC    - `total_daily_orders`: Count of orders per date
-- MAGIC    - `unique_customers`: Distinct customers per date
-- MAGIC 3. Notice: This is clean, business-ready data!
-- MAGIC
-- MAGIC ### Step 3: Review Performance
-- MAGIC
-- MAGIC 1. Click the **Performance** tab in the bottom panel
-- MAGIC 2. Review execution metrics:
-- MAGIC    - Total duration: ~2-3 minutes (first run)
-- MAGIC    - Per-table execution time
-- MAGIC    - Resource usage
-- MAGIC 3. Click any table to see its query profile

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## J. Understanding Incremental Processing
-- MAGIC
-- MAGIC One of the key benefits of Lakeflow Pipelines is **automatic incremental processing**.
-- MAGIC
-- MAGIC ### Run the Pipeline Again
-- MAGIC
-- MAGIC 1. Click **Run pipeline** again
-- MAGIC 2. Wait for it to complete (should be very fast - seconds)
-- MAGIC 3. Observe:
-- MAGIC    - **0 new rows** in all tables
-- MAGIC    - Execution is much faster
-- MAGIC
-- MAGIC **Why?** Auto Loader tracks which files have been processed and skips them!
-- MAGIC
-- MAGIC ### Add New Data
-- MAGIC
-- MAGIC Let's add a new file and see incremental processing in action:

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import sys, os, re
-- MAGIC
-- MAGIC # Determine pipeline root automatically (one level up from current file)
-- MAGIC pipeline_root = os.path.dirname(os.getcwd())
-- MAGIC sys.path.append(pipeline_root)
-- MAGIC
-- MAGIC # Now import your helper
-- MAGIC from utilities.utils import add_orders_file   # or utilities.utils if that's your filename
-- MAGIC
-- MAGIC # Get current user information
-- MAGIC current_user = spark.sql("SELECT current_user()").collect()[0][0]
-- MAGIC username = current_user.split("@")[0]
-- MAGIC
-- MAGIC # Clean username for use in naming (remove special characters)
-- MAGIC clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())
-- MAGIC
-- MAGIC working_dir = f'/Volumes/sdp_workshop_{clean_username}/default/raw'
-- MAGIC
-- MAGIC result = add_orders_file(spark, working_dir, file_number=1, num_orders=25)
-- MAGIC print(result)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Run Pipeline with New Data
-- MAGIC
-- MAGIC 1. Go back to your **pipeline editor tab**
-- MAGIC 2. Click **Run pipeline** again
-- MAGIC 3. Watch the pipeline graph
-- MAGIC 4. Observe:
-- MAGIC    - **+25 rows** processed in bronze and silver
-- MAGIC    - Gold layer recomputed efficiently
-- MAGIC    - Only the NEW file (01.json) was read
-- MAGIC
-- MAGIC **This is incremental processing!** The pipeline:
-- MAGIC - Detects new files automatically
-- MAGIC - Processes only new data
-- MAGIC - Updates downstream tables
-- MAGIC - All without manual intervention

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## K. Query Your Pipeline Tables
-- MAGIC
-- MAGIC The tables created by your pipeline are available in Unity Catalog. You can query them like any other table!

-- COMMAND ----------

-- DBTITLE 1,Set Schema and Catalog
-- MAGIC %py
-- MAGIC # Set catalog for SQL queries in this notebook
-- MAGIC import re
-- MAGIC current_user = spark.sql("SELECT current_user()").collect()[0][0]
-- MAGIC username = current_user.split("@")[0]
-- MAGIC clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())
-- MAGIC catalog_name = f"sdp_workshop_{clean_username}"
-- MAGIC
-- MAGIC # Set as default catalog for all queries
-- MAGIC spark.sql(f"USE CATALOG {catalog_name}")
-- MAGIC print(f"✓ Using catalog: {catalog_name}")
-- MAGIC print("  All SQL queries will use this catalog automatically")

-- COMMAND ----------

-- Query the bronze table
SELECT * FROM bronze.orders LIMIT 10;

-- COMMAND ----------

-- Query the silver table
SELECT order_id, order_timestamp, customer_id 
FROM silver.orders_clean 
ORDER BY order_timestamp DESC
LIMIT 10;

-- COMMAND ----------

-- Query the gold aggregation
SELECT * FROM gold.order_summary 
ORDER BY order_date;

-- COMMAND ----------

-- Check total row counts
SELECT 
  'orders' AS table_name, COUNT(*) AS row_count FROM bronze.orders
UNION ALL
SELECT 
  'orders_clean' AS table_name, COUNT(*) AS row_count FROM silver.orders_clean
UNION ALL
SELECT 
  'order_summary' AS table_name, COUNT(*) AS row_count FROM gold.order_summary;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## L. Key Takeaways - Lesson 1
-- MAGIC
-- MAGIC ✅ **Lakeflow Spark Declarative Pipelines** provide a declarative framework for ETL
-- MAGIC
-- MAGIC ✅ **Multi-file editor** offers an IDE-like experience with integrated monitoring
-- MAGIC
-- MAGIC ✅ **Streaming tables** handle incremental processing automatically
-- MAGIC
-- MAGIC ✅ **Materialized views** provide efficient aggregations and joins
-- MAGIC
-- MAGIC ✅ **Data quality expectations** enforce standards at ingestion time
-- MAGIC
-- MAGIC ✅ **Auto Loader** (`read_files`) simplifies file ingestion with checkpointing
-- MAGIC
-- MAGIC ✅ **Medallion architecture** organizes data into Bronze → Silver → Gold layers
-- MAGIC
-- MAGIC ## What's Next?
-- MAGIC
-- MAGIC In **Lesson 2**, we'll build on these concepts to create production pipelines with:
-- MAGIC - Multiple source code files
-- MAGIC - Cross-file dependencies
-- MAGIC - Joining streaming tables
-- MAGIC - Scheduling and monitoring
-- MAGIC - Change Data Capture (CDC)