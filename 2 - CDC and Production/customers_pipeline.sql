-------------------------------------------------------
-- CUSTOMERS PIPELINE WITH AUTO CDC
-- Lakeflow Spark Declarative Pipelines
-------------------------------------------------------
-- This file demonstrates Change Data Capture (CDC)
-- using the AUTO CDC INTO feature for SCD Type 1
-- 
-- AUTO CDC automatically handles:
-- - INSERT operations (new customers)
-- - UPDATE operations (customer changes)
-- - DELETE operations (customer removals)
-------------------------------------------------------

-------------------------------------------------------
-- BRONZE LAYER: Ingest Raw CDC Data
-------------------------------------------------------
-- Ingest CDC events from source system
-- Each record contains: data fields + operation type
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze.customers_raw
  COMMENT "Raw CDC events for customer data"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false
  )
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/customers",
  format => 'json'
);

-------------------------------------------------------
-- BRONZE LAYER: Clean and Validate CDC Data
-------------------------------------------------------
-- Add data quality checks before applying CDC
-- Transform timestamp field for proper sequencing
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE bronze.customers_clean
  (
    -- Critical validations - fail the update if violated
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
    
    -- Business rule validations - data quality dependent on operation
    CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = 'DELETE'),
    CONSTRAINT valid_address EXPECT (
      (address IS NOT NULL AND 
       city IS NOT NULL AND 
       state IS NOT NULL AND 
       zip_code IS NOT NULL) OR
      operation = 'DELETE'
    ),
    
    -- Email format validation - drop invalid rows
    CONSTRAINT valid_email EXPECT (
      rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') OR 
      operation = 'DELETE'
    ) ON VIOLATION DROP ROW
  )
  COMMENT "Validated CDC events ready for processing"
  TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *,
  -- Convert Unix timestamp to proper timestamp for sequencing
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM bronze.customers_raw;

-------------------------------------------------------
-- SILVER LAYER: Apply CDC with AUTO CDC INTO
-------------------------------------------------------
-- Create the target table for SCD Type 1
-- This will be automatically updated by the CDC flow
-------------------------------------------------------

CREATE OR REFRESH STREAMING TABLE silver.customers
  COMMENT "Current state of customers (SCD Type 1)";

-------------------------------------------------------
-- CREATE FLOW: Auto CDC Processing
-------------------------------------------------------
-- The flow automatically processes CDC operations:
-- - INSERT: Adds new customers
-- - UPDATE: Modifies existing customers (overwrites)
-- - DELETE: Removes customers from target table
-------------------------------------------------------

CREATE FLOW customers_cdc_flow AS 
AUTO CDC INTO silver.customers     -- Target table to maintain
FROM STREAM bronze.customers_clean      -- Source CDC events
  KEYS (customer_id)                                -- Primary key for matching records
  APPLY AS DELETE WHEN operation = 'DELETE'         -- Handle delete operations
  SEQUENCE BY timestamp_datetime                    -- Order of operations
  COLUMNS * EXCEPT (timestamp, _rescued_data, operation, processing_time, source_file)
  STORED AS SCD TYPE 1;                             -- Slowly Changing Dimension Type 1


-------------------------------------------------------
-- GOLD LAYER: Customer Analytics
-------------------------------------------------------
-- Create business views from the current customer state
-------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW gold.customer_summary
  COMMENT "Customer summary with derived metrics"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
  customer_id,
  name,
  state,
  city,
  -- Can add joins here with orders/status if needed
  current_timestamp() AS last_refreshed
FROM silver.customers;

-------------------------------------------------------
-- KEY CONCEPTS: AUTO CDC INTO
-- 
-- 1. CDC Operations:
--    - operation = 'INSERT': New record
--    - operation = 'UPDATE': Modified record  
--    - operation = 'DELETE': Removed record
-- 
-- 2. KEYS Clause:
--    - Defines primary key for matching records
--    - Used to identify which row to update/delete
--    - Can be single column or composite key
-- 
-- 3. SEQUENCE BY:
--    - Determines order of applying changes
--    - Critical for handling out-of-order events
--    - Usually a timestamp or sequence number
-- 
-- 4. SCD TYPE 1 vs TYPE 2:
--    - TYPE 1: Overwrites old values (current state only)
--    - TYPE 2: Preserves history (not covered in this workshop)
--         Use STORED AS SCD TYPE 2 for historical tracking
-- 
-- 5. COLUMNS EXCEPT:
--    - Excludes metadata columns from target table
--    - Keeps target table clean with only business data
-- 
-- 6. Data Quality with CDC:
--    - Validate BEFORE applying CDC
--    - Bad data can corrupt your target table
--    - Use FAIL UPDATE for critical constraints
-- 
-- 7. Benefits of AUTO CDC:
--    - No manual MERGE logic needed
--    - Handles out-of-order events automatically
--    - Built-in deduplication
--    - Optimized for performance
-------------------------------------------------------


-------------------------------------------------------
-- EXAMPLE: How AUTO CDC Works
-------------------------------------------------------
-- Given these CDC events in order:
--
-- Event 1: operation='INSERT'
--   customer_id='C001', name='John Doe', email='john@example.com'
--   Result: New row added to target table
--
-- Event 2: operation='UPDATE'
--   customer_id='C001', name='John Smith', email='john.smith@example.com'
--   Result: Existing row updated (name and email changed)
--
-- Event 3: operation='DELETE'
--   customer_id='C001'
--   Result: Row removed from target table
--
-- The target table always reflects the CURRENT state
-- No history is maintained (that's SCD Type 1)
-------------------------------------------------------


-------------------------------------------------------
-- OPTIONAL ENHANCEMENT: Add SCD Type 2 for History
-------------------------------------------------------
-- If you want to track history, create a separate flow:
--
-- CREATE OR REFRESH STREAMING TABLE 2_silver_db.customers_history
--   COMMENT "Customer history (SCD Type 2)";
--
-- CREATE FLOW customers_cdc_flow_scd2 AS 
-- AUTO CDC INTO 2_silver_db.customers_history
-- FROM STREAM bronze.customers_clean
--   KEYS (customer_id)
--   APPLY AS DELETE WHEN operation = 'DELETE'
--   SEQUENCE BY timestamp_datetime
--   COLUMNS * EXCEPT (timestamp, _rescued_data, operation)
--   STORED AS SCD TYPE 2;
--
-- SCD Type 2 adds:
-- - __START_AT: When this version became active
-- - __END_AT: When this version was superseded
-- - __CURRENT: Boolean indicating current version
-------------------------------------------------------
