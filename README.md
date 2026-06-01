# Lakeflow Spark Declarative Pipelines Workshop

A hands-on companion for learning Databricks Lakeflow Spark Declarative Pipelines (SDP, formerly Delta Live Tables).

This workshop is now a single self-contained Markdown file, [`Lakeflow-SDP-Get-Started.md`](Lakeflow-SDP-Get-Started.md). It is a fork of the official Microsoft Learn Lakeflow tutorials with a workshop teaching layer added on top. For the full set of official tutorials it draws from, see the parent page: [Lakeflow Declarative Pipelines tutorials](https://learn.microsoft.com/en-us/azure/databricks/ldp/tutorials).

## What is inside

- [`Lakeflow-SDP-Get-Started.md`](Lakeflow-SDP-Get-Started.md): the complete workshop. It follows the MS Learn get-started flow (create a pipeline, apply data-quality expectations, analyse top users), reuses the official code samples and diagrams, and adds workshop-only callouts: learning objectives, the `transformations/` folder model, the three expectation violation modes, the full-refresh guard, inner-loop tooling, triggered vs continuous execution, a production CDC overview, and key takeaways.
- `images/`: diagrams referenced by the workshop file.

## How to use it

1. Open [`Lakeflow-SDP-Get-Started.md`](Lakeflow-SDP-Get-Started.md) and read it top to bottom.
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
