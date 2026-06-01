# Lakeflow Spark Declarative Pipelines Workshop

A hands-on companion for learning Databricks Lakeflow Spark Declarative Pipelines (SDP, formerly Delta Live Tables).

This workshop is now a single self-contained Markdown file, [`Lakeflow-SDP-CDC-Pipeline.md`](Lakeflow-SDP-CDC-Pipeline.md). It is a fork of the official Microsoft Learn Lakeflow tutorials with a workshop teaching layer added on top. For the full set of official tutorials it draws from, see the parent page: [Lakeflow Declarative Pipelines tutorials](https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorials).

## What is inside

- [`Lakeflow-SDP-CDC-Pipeline.md`](Lakeflow-SDP-CDC-Pipeline.md): the complete workshop. It follows the MS Learn change-data-capture pipeline flow (ingest with Auto Loader into bronze, clean to silver with expectations, `AUTO CDC INTO` a current-state customers table, build an SCD2 history table, aggregate in gold, and schedule as a job), reuses the official code samples and the pipeline diagram, and adds workshop-only callouts: learning objectives, the `transformations/` folder model, the three expectation violation modes, an AUTO CDC clause-by-clause breakdown, SCD Type 1 vs Type 2, an incremental-processing demo, the full-refresh guard, inner-loop tooling, triggered vs continuous execution, and key takeaways.
- `images/`: diagrams referenced by the workshop file.

## How to use it

1. Open [`Lakeflow-SDP-CDC-Pipeline.md`](Lakeflow-SDP-CDC-Pipeline.md) and read it top to bottom.
2. Follow the steps in an Azure Databricks workspace with Unity Catalog enabled.
3. Lean on the marked Workshop add-on callouts for the concepts and decision frameworks that the base tutorial does not cover.

## Requirements

- An Azure Databricks workspace with Unity Catalog enabled.
- Permission to create a compute resource, or access to one.
- Permission to create a schema in a catalog (`ALL PRIVILEGES`, or `USE CATALOG` plus `CREATE SCHEMA`).

## Source and attribution

The Azure Databricks documentation is authored by Databricks and published to Microsoft Learn. This workshop adapts those tutorials for internal training use, with documentation-owner sign-off. Sections that come from the official flow are adapted; everything marked Workshop add-on is added by us. Parent tutorials page: https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorials

---

Version: 2.0
Last updated: June 2026
