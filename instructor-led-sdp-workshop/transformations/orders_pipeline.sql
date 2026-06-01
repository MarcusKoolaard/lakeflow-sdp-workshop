-------------------------------------------------------
-- ORDERS PIPELINE
-- Lakeflow Spark Declarative Pipelines
-------------------------------------------------------
-- This file is part of a multi-file pipeline project.
-- The pipeline editor automatically discovers and 
-- combines all SQL files in your pipeline.
-------------------------------------------------------

-------------------------------------------------------
-- BRONZE LAYER: Ingest Raw JSON Data
-------------------------------------------------------
-- Incrementally ingest JSON files from cloud storage
-- using Auto Loader for efficient file processing
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze.orders -- inherit default catalog but specifying bronze schema
  COMMENT "Raw orders data ingested from JSON files"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false  -- Prevent accidental full refreshes
  )
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files( -- Incrementally process new files with Auto Loader
  "${source}/orders",  -- Uses the 'source' configuration variable from pipeline settings
  format => 'json'
);

-------------------------------------------------------
-- SILVER LAYER: Clean and Transform
-------------------------------------------------------
-- Parse timestamp and select relevant columns
-- This creates a clean, validated streaming table
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver.orders_clean -- Publish to multiple catalogs and schemas
  (
    -- Data quality expectations
    CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL),
    CONSTRAINT valid_timestamp EXPECT (order_timestamp > "2020-01-01")
  )
  COMMENT "Clean orders data with validated fields"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  notifications
FROM STREAM bronze.orders;

-------------------------------------------------------
-- GOLD LAYER: Business Aggregation
-------------------------------------------------------
-- Create a materialized view for daily order summary
-- Materialized views automatically optimize refresh
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.order_summary
  COMMENT "Daily order counts aggregated from silver layer"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders,
  count(DISTINCT customer_id) AS unique_customers
FROM silver.orders_clean
GROUP BY date(order_timestamp);

-------------------------------------------------------
-- NOTES:
-- 1. This file works standalone or as part of a larger pipeline
-- 2. Tables from this file can be referenced by other files
-- 3. Variable substitution: ${source} is replaced at runtime
-- 4. Streaming tables use checkpoints for incremental processing
-- 5. Materialized views handle full refreshes efficiently
-------------------------------------------------------
