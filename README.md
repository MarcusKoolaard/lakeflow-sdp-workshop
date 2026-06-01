# Lakeflow Spark Declarative Pipelines Workshops

A hands-on companion for learning Databricks Lakeflow Spark Declarative Pipelines (SDP, formerly Delta Live Tables).

This repository offers the workshop in two formats: 
1. **Start here** - Self-paced single-file workshop, [`Lakeflow-SDP-CDC-Pipeline.md`](Lakeflow-SDP-CDC-Pipeline.md) - last updated June 2026
2. Instructor-led course under [`instructor-led-sdp-workshop/`](instructor-led-sdp-workshop/) - last updated November 2025

## What is inside

- [`Lakeflow-SDP-CDC-Pipeline.md`](Lakeflow-SDP-CDC-Pipeline.md): the complete workshop. Based on [Tutorial: Build an ETL pipeline using change data capture](https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorial-pipelines). It follows a change-data-capture pipeline flow (ingest with Auto Loader into bronze, clean to silver with expectations, `AUTO CDC INTO` a current-state customers table, build an SCD2 history table, aggregate in gold, and schedule as a job), with learning objectives, the `transformations/` folder model, the three expectation violation modes, an AUTO CDC clause-by-clause breakdown, SCD Type 1 vs Type 2, an incremental-processing demo, the full-refresh guard, inner-loop tooling, triggered vs continuous execution, and key takeaways.
- [`instructor-led-sdp-workshop/`](instructor-led-sdp-workshop/): the original multi-notebook, instructor-led course (Setup, Exercise 1, Exercise 2, plus `transformations/` and `utilities/`). See its own [README](instructor-led-sdp-workshop/README.md).

## How to use it

1. Open [`Lakeflow-SDP-CDC-Pipeline.md`](Lakeflow-SDP-CDC-Pipeline.md) and read it top to bottom.
2. Follow the steps in an Azure Databricks workspace.
3. Note workshop add-on callouts for key concepts and decision frameworks.

Version: 2.0
Last updated: June 2026
