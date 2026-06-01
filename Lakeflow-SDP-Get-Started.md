> **⚠️ This document is a fork of the Microsoft Learn tutorial
> [_Create your first pipeline using the Lakeflow Pipelines Editor_](https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorial-get-started).**
> The step-by-step flow, code samples, and screenshots follow that tutorial; the narrative
> has been rewritten for this workshop. Everything marked **🧩 Workshop add-on** below is
> *added* material that is **not** in the original Microsoft Learn page — it ports the
> teaching layer from the internal Lakeflow SDP Workshop on top of the official flow.
> Source last reviewed: 2026-05-12. Attribution preserved per CC BY 4.0.
>
> _Legend:_ plain sections = adapted from MS Learn · **🧩 Workshop add-on** = added by us.

---

# Tutorial: Create your first pipeline using the Lakeflow Pipelines Editor

This tutorial walks you through building a pipeline with **Lakeflow Spark Declarative
Pipelines (SDP)** and the **Lakeflow Pipelines Editor**. You start from a sample pipeline,
clean the data with a quality constraint, then add a transformation that surfaces the
top 100 users by booking count — all using Auto Loader for incremental ingestion.

You will use the Lakeflow Pipelines Editor to:

- Create a pipeline from the default folder structure and the built-in sample files.
- Enforce data-quality rules with **expectations**.
- Extend the pipeline with a new transformation that analyses your data.

> ### 🧩 Workshop add-on — Learning objectives
> By the end of this tutorial you should be able to:
> - Stand up a Lakeflow SDP pipeline from the multi-file editor and run it.
> - Explain how the `transformations/` folder turns files into a dependency graph.
> - Choose the right **expectation violation mode** for a given rule.
> - Use the editor's inner-loop tools (dry run, single-table run) instead of always
>   running the whole pipeline.
> - Recognise when a **full-refresh guard** and **execution mode** matter for production.

## Requirements

Before you start, make sure you:

- Are signed in to an Azure Databricks workspace.
- Have **Unity Catalog** enabled on the workspace.
- Can create — or already have access to — a compute resource.
- Can create a schema in a catalog. You need either `ALL PRIVILEGES`, or both
  `USE CATALOG` and `CREATE SCHEMA`.

## Step 1: Create a pipeline

In this step you create a pipeline from the default folder layout and the bundled code
samples. The samples read the `users` table from the `wanderbricks` sample data source.

1. In the workspace, click <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/product-icons/plusicon.svg" alt="Plus icon" height="14"> **New**, then <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/product-icons/pipelineicon.svg" alt="Pipeline icon" height="14"> **ETL pipeline**. The editor opens with a default name such as `New Pipeline <date> <time>`.
2. *(Optional)* Click the name to rename the pipeline to something descriptive.
3. *(Optional)* Next to the name, click the catalog and schema to change the defaults.
4. *(Optional)* In the generated `my_transformation` file, pick **Python** or **SQL** from the language drop-down.
5. Click <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/product-icons/codeicon.svg" alt="Code icon" height="14"> **Use sample code**. Sample code in your chosen language appears inside the `transformations` folder. The output datasets don't exist yet, so the **Pipeline graph** on the right is still empty.
6. Click **Run pipeline** (top-right) to execute everything in the `transformations` folder. When the run finishes, two new tables appear — `sample_users_<date_time>` and `sample_aggregation_<date_time>` — and the graph shows `sample_users` feeding `sample_aggregation`. **Note the full `sample_users_<date_time>` name; you reference it in Step 2.**

> ### 🧩 Workshop add-on — The `transformations/` folder *is* the pipeline
> Every new Lakeflow pipeline ships with a `transformations/` folder. **All `*.py` and
> `*.sql` files inside it are auto-discovered as pipeline source.** Lakeflow reads the
> dataset dependencies across those files and builds the DAG itself — so **file order and
> file count don't matter**, and a dataset in one file can reference a dataset defined in
> another. Files that live in the pipeline *root* (outside `transformations/`) are ignored
> at runtime. This is why you can split a pipeline across many files and why a standalone
> `.sql` file "just works" the moment you drop it into `transformations/`.
> Refs: [Lakeflow Pipelines Editor](https://docs.databricks.com/aws/en/ldp/multi-file-editor) ·
> [Configure pipelines](https://docs.databricks.com/aws/en/ldp/configure-pipeline).

## Step 2: Apply data quality checks

Now add a quality check to `sample_users`. You use
[pipeline expectations](https://learn.microsoft.com/en-us/azure/databricks/ldp/expectations)
to constrain the data — here you drop any record without a valid email and write the
cleaned result to `users_cleaned`.

1. In the [pipeline asset browser](https://learn.microsoft.com/en-us/azure/databricks/ldp/multi-file-editor#pipeline-assets-browser) on the left, click <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/product-icons/plusicon.svg" alt="Plus icon" height="14"> and choose **Transformation**.
2. In **Create new transformation file**:
   - Pick **Python** or **SQL** for the language (it need not match Step 1).
   - Name the file `users_cleaned`.
   - Leave **Destination path** at its default.
   - For **Dataset type**, leave **None selected**, or choose **Materialized view** to get starter code.
3. Click **Create**.
4. Edit the file to match the code below for your language, replacing `sample_users_<date_time>` with the full table name you noted in Step 1.

### SQL

```sql
-- Drop all rows that do not have an email address

CREATE MATERIALIZED VIEW users_cleaned
(
  CONSTRAINT non_null_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW
) AS
SELECT *
FROM sample_users_<date_time>;
```

### Python

```python
from pyspark import pipelines as dp

# Drop all rows that do not have an email address

@dp.materialized_view
@dp.expect_or_drop("no null emails", "email IS NOT NULL")
def users_cleaned():
    return (
        spark.read.table("sample_users_<date_time>")
    )
```

5. Click **Run pipeline**. The pipeline now has three tables.

> ### 🧩 Workshop add-on — The three expectation violation modes
> This tutorial uses `DROP ROW`, but expectations support three behaviours. Pick by how
> serious the rule is:
>
> | Mode | What happens to bad rows | What happens to the run | Use when… |
> |---|---|---|---|
> | `ON VIOLATION FAIL UPDATE` | — | **Run fails immediately** | The rule is a hard invariant (e.g. primary key must not be null). |
> | `ON VIOLATION DROP ROW` | Dropped from the target | Run continues | You want clean output and can safely discard bad records (this tutorial's email check). |
> | *(no `ON VIOLATION`)* — default | **Kept** in the target | Run continues | You only want to *measure* data health — violations are recorded as metrics/warnings, data is unchanged. |
>
> SQL uses the `ON VIOLATION …` clause; Python uses `@dp.expect_or_fail`,
> `@dp.expect_or_drop`, and `@dp.expect` respectively.
> Workshop example combining `FAIL UPDATE` + warn-only:
> `transformations/orders_pipeline.sql:43-45`.

> ### 🧩 Workshop add-on — Guard against accidental full refreshes
> A **full refresh** drops a streaming table's state and re-ingests everything from source —
> expensive, and destructive for append-only history. You can block it per table:
> ```sql
> TBLPROPERTIES ("pipelines.reset.allowed" = false)
> ```
> Add this to streaming tables you never want rebuilt by an over-eager click.
> See workshop `transformations/orders_pipeline.sql:21` and
> [when to full-refresh](https://docs.databricks.com/aws/en/ldp/updates).

## Step 3: Analyze top users

Finally, find the top 100 users by number of bookings by joining the
`wanderbricks.bookings` table to your `users_cleaned` materialized view.

1. In the pipeline asset browser, click <img src="https://learn.microsoft.com/en-us/azure/databricks/_static/images/product-icons/plusicon.svg" alt="Plus icon" height="14"> and choose **Transformation**.
2. In **Create new transformation file**:
   - Pick **Python** or **SQL** (again, it need not match earlier choices).
   - Name the file `users_and_bookings`.
   - Leave **Destination path** at its default.
   - Leave **Dataset type** as **None selected**.
3. Click **Create**.
4. Edit the file to match the code below for your language.

### SQL

```sql
-- Get the top 100 users by number of bookings

CREATE OR REFRESH MATERIALIZED VIEW users_and_bookings AS
SELECT u.name AS name, COUNT(b.booking_id) AS booking_count
FROM users_cleaned u
JOIN samples.wanderbricks.bookings b ON u.user_id = b.user_id
GROUP BY u.name
ORDER BY booking_count DESC
LIMIT 100;
```

### Python

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, desc

# Get the top 100 users by number of bookings

@dp.materialized_view
def users_and_bookings():
    return (
        spark.read.table("users_cleaned")
        .join(spark.read.table("samples.wanderbricks.bookings"), "user_id")
        .groupBy(col("name"))
        .agg(count("booking_id").alias("booking_count"))
        .orderBy(desc("booking_count"))
        .limit(100)
    )
```

5. Click **Run pipeline**. When the run completes, the **Pipeline graph** shows four tables, including the new `users_and_bookings`.

![Pipeline graph showing four tables in pipeline](https://learn.microsoft.com/en-us/azure/databricks/_static/images/dlt/tutorial-get-started-final-graph.png)

> ### 🧩 Workshop add-on — Tighten your inner loop (don't always "Run pipeline")
> The tutorial only shows **Run pipeline**, which materialises everything. While developing,
> two editor tools are far faster:
> - **Dry run** — validates syntax and the dataset dependency graph **without** writing any
>   data. Use it to catch typos and broken references in seconds before a real run.
> - **Run one table at a time** (selective run) — executes a *single* dataset instead of the
>   whole DAG. Ideal when you're iterating on one transformation and don't want to rebuild
>   upstream tables every time.
>
> Workshop walk-through: `1 - Building Pipeline with Data Quality.sql:257-315`.

> ### 🧩 Workshop add-on — Triggered vs Continuous execution
> When you move from "run it once" to production, you choose an execution mode:
>
> | | **Triggered** | **Continuous** |
> |---|---|---|
> | Behaviour | Runs to completion, then stops | Keeps running, processes data as it arrives |
> | Latency | Batch (minutes–hours) | Low (seconds) |
> | Cost | Pay per run | Always-on compute |
> | Default / good for | Most ETL; scheduled refreshes | Real-time / low-latency needs |
>
> Start with **Triggered** unless you have a real latency requirement. See
> [batch vs streaming recommendations](https://docs.databricks.com/aws/en/data-engineering/batch-vs-streaming#recommendations).

> ### 🧩 Workshop add-on — Key takeaways
> - A Lakeflow pipeline is **declarative**: you describe datasets, Lakeflow builds and orders
>   the DAG from the files in `transformations/`.
> - **Materialized views** recompute efficiently; **streaming tables** + Auto Loader process
>   data incrementally.
> - **Expectations** are your data-quality contract — choose `FAIL UPDATE` / `DROP ROW` /
>   default deliberately.
> - Develop with **dry run** and **single-table run**; protect production tables with
>   `pipelines.reset.allowed = false`.
> - Production pipelines run on a chosen **execution mode** and are orchestrated by
>   Lakeflow Jobs.

## Where to go next

> ### 🧩 Workshop add-on — Curated next steps & references
> | Topic | Link |
> |---|---|
> | Original tutorial (this fork's source) | https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorial-get-started |
> | Lakeflow Pipelines Editor + `transformations/` folder | https://docs.databricks.com/aws/en/ldp/multi-file-editor |
> | Configure pipelines (source paths, root folder) | https://docs.databricks.com/aws/en/ldp/configure-pipeline |
> | When to (and not to) full-refresh | https://docs.databricks.com/aws/en/ldp/updates |
> | Batch vs streaming decision rubric | https://docs.databricks.com/aws/en/data-engineering/batch-vs-streaming#recommendations |
> | `pyspark.pipelines` Python dev guide | https://docs.databricks.com/aws/en/ldp/developer/python-dev |
> | Where is DLT? (rename + soft-deprecation) | https://docs.databricks.com/aws/en/ldp/where-is-dlt |
> | Tutorial — create a source-controlled pipeline | https://learn.microsoft.com/en-us/azure/databricks/ldp/source-controlled |
> | Tutorial — convert a pipeline to a bundle | https://learn.microsoft.com/en-us/azure/databricks/ldp/convert-to-dab |
>
> For the deep, classroom version of this material — CDC with `AUTO CDC INTO`, SCD Type 1 vs
> Type 2, production scheduling, and a hands-on incremental-processing demo — see the full
> **Lakeflow SDP Workshop** in this repository.
