# Lakeflow Spark Declarative Pipelines Workshop

A hands-on 1.5-hour workshop teaching Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables) with a focus on data quality, Change Data Capture, and production best practices.

## Workshop Structure

```
workshop/
├── Setup/
│   └── 0-SETUP.py                          # Run this FIRST - creates catalog, schemas, volume, sample data
│
├── Exercise_1/ (40 minutes)
│   └── 1-Building_Pipelines_with_Data_Quality.sql   # Notebook with instructions
│
├── Exercise_2/ (50 minutes)
│   ├── 2-Production_CDC_and_Scheduling.sql  # Notebook with instructions  
│   └── customers_pipeline.sql               # Move this to /transformations during exercise
│
└── transformations/
    └── orders_pipeline.sql                  # Already here for Exercise 1
    (customers_pipeline.sql added here during Exercise 2)
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- CREATE CATALOG privileges
- Serverless compute enabled (recommended) or access to create clusters

## Getting Started

### Step 1: Import Workshop Files

1. In Databricks workspace, navigate to your home directory
2. Import all workshop files maintaining the folder structure above
3. You should have: `Setup/`, `Exercise_1/`, `Exercise_2/`, and `transformations/` folders

### Step 2: Run Setup

1. Open `Setup/0-SETUP.py`
2. Run ALL cells in order
3. **Save the output values** - you'll need them for pipeline configuration:
   - Default Catalog: `sdp_workshop_<your_username>`
   - Default Schema: `bronze`
   - Configuration Variable `source`: `/Volumes/sdp_workshop_<your_username>/default/raw`

### Step 3: Exercise 1 - Building Pipelines with Data Quality

**Duration:** ~50 minutes

**What you'll learn:**
- Create a Lakeflow Spark Declarative Pipeline
- Implement medallion architecture (Bronze → Silver → Gold)
- Apply data quality expectations
- Use Auto Loader for incremental ingestion
- Work with the multi-file pipeline editor

**Files used:**
- `Exercise_1/1-Building_Pipelines_with_Data_Quality.sql` (instructions)
- `transformations/orders_pipeline.sql` (pipeline code)

**Key steps:**
1. Open the Exercise 1 notebook
2. Follow instructions to create your first pipeline
3. Configure pipeline settings with values from setup
4. Run the pipeline and observe results

### Step 4: Exercise 2 - Change Data Capture and Production

**Duration:** ~60 minutes

**What you'll learn:**
- Implement Change Data Capture using AUTO CDC INTO
- Understand SCD Type 1 slowly changing dimensions
- Handle INSERT, UPDATE, DELETE operations automatically
- Add new sources to existing pipelines
- Schedule pipelines for production
- Apply production best practices

**Files used:**
- `Exercise_2/2-Production_CDC_and_Scheduling.sql` (instructions)
- `Exercise_2/customers_pipeline.sql` (you'll move this to transformations/)

**Key steps:**
1. Open the Exercise 2 notebook
2. Review the customers_pipeline.sql code
3. **Move** customers_pipeline.sql from Exercise_2/ to transformations/
4. Pipeline automatically picks up the new file
5. Generate sample CDC data and run pipeline
6. Schedule pipeline for production

## What's Created

### Data Architecture

**Catalog:** `sdp_workshop_<username>`

**Schemas:**
- `bronze` - Raw ingestion layer
- `silver` - Cleaned and validated data
- `gold` - Business aggregations

**Tables (Exercise 1):**
- `bronze.orders` - Raw order JSON
- `silver.orders_clean` - Validated orders
- `gold.order_summary` - Daily aggregations

**Tables (Exercise 2 adds):**
- `bronze.customers_raw` - Raw CDC events
- `bronze.customers_clean` - Validated CDC events
- `silver.customers` - Current customer state (SCD Type 1)
- `gold.customer_summary` - Customer analytics

**Volume:**
- `sdp_workshop_<username>.default.raw` - Landing zone for source files
  - `orders/` - Order JSON files
  - `status/` - Status update JSON files (for future use)
  - `customers/` - Customer CDC events

## Key Concepts Covered

### Exercise 1
- **Streaming Tables** - Incrementally updated tables
- **Materialized Views** - Efficient aggregations
- **Auto Loader** - `read_files()` for incremental file ingestion
- **Data Quality Expectations** - Enforce data quality at ingestion
- **Medallion Architecture** - Bronze → Silver → Gold pattern
- **Multi-file Editor** - IDE-like pipeline development

### Exercise 2
- **AUTO CDC INTO** - Declarative Change Data Capture
- **SCD Type 1** - Current state tracking
- **KEYS clause** - Primary key matching
- **SEQUENCE BY** - Event ordering
- **APPLY AS DELETE** - Deletion handling
- **Production Scheduling** - Continuous and scheduled modes
- **Monitoring** - Data quality metrics and performance

## Best Practices Demonstrated

1. **Data Quality** - Expectations at bronze and silver layers
2. **Reset Prevention** - `pipelines.reset.allowed = false`
3. **Incremental Processing** - Only process new data
4. **Validation Before CDC** - Always validate before applying changes
5. **Metadata Tracking** - Source file names, processing times
6. **Production Settings** - Scheduling, notifications, monitoring
7. **Documentation** - Comments and table properties

## Troubleshooting

### "Variable 'source' not found"
- Go to Pipeline Settings → Configuration
- Add key: `source`, value: path from setup output

### "Schema not found"
- Verify setup notebook ran successfully
- Check catalog name in pipeline settings

### "Permission denied"
- Ensure you have CREATE privileges in Unity Catalog
- Contact workspace admin if needed

### Pipeline not finding new files
- Check volume path is correct
- Verify files exist: Use Databricks file browser
- Ensure file format matches (JSON)

### CDC not applying changes correctly
- Verify SEQUENCE BY column has proper timestamps
- Check KEYS clause matches your primary key
- Ensure operation column values are correct (INSERT/UPDATE/DELETE)

## Additional Resources

- [Lakeflow Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
- [AUTO CDC INTO Reference](https://docs.databricks.com/en/delta-live-tables/cdc.html)
- [Data Quality Expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)

## Workshop Timeline

| Time | Activity | Files |
|------|----------|-------|
| 0:00-0:10 | Setup & Introduction | 0-SETUP.py |
| 0:10-0:50 | Exercise 1: Building Pipelines with Data Quality | 1 - Building Pipelines with Data Quality.sql |
| 0:50-1:40 | Exercise 2: CDC & Production | 2 - CDC and Production.sql |
| 1:40-2:00 | Q&A / Overflow Time | |

## Support

For questions or issues with this workshop:
1. Review the troubleshooting section above
2. Check Databricks documentation
3. Contact your workshop instructor or Databricks support

---

**Version:** 1.0  
**Last Updated:** November 2025  