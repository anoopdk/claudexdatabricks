# OBT Ecommerce Medallion on Databricks SDP

> Zynx Technologies | 3-slide delivery overview

---

## Slide 1: Context & problem

- Zynx x Retail Client
- Engagement: Implementation Delivery
- The Retail Client received denormalised One-Big-Table ecommerce Excel extracts and needed a governed Databricks Lakeflow SDP medallion path that splits domains, applies SCD logic, and publishes gold analytics without manual spreadsheet modelling.
- Engagement levels: Source Profiling (OBT Excel) -> Architecture & Layer Design -> SDP Pipeline Implementation -> Job Orchestration & Handover

---

## Slide 2: Solution architecture & stack

- Zynx implemented Bronze -> Silver Core -> Silver Dimensional -> Gold SDP pipelines with Unity Catalog schemas, expectations, SCD2 dimensions, star-schema facts, and a sequenced job.
- Stack: Databricks Lakeflow SDP, Unity Catalog, Auto Loader / volumes, SCD Type 2, Python pipelines, Asset Bundle resources
- Architecture documented in docs/03-architecture.md with Mermaid diagrams

---

## Slide 3: Implementation highlights & inspection

- Key evidence: docs/technical-architecture.md, src/pipelines/, resources/, databricks.yml
- Open docs/audit/PROJECT-FACTSHEET.md for the audit index
- Repository standardised to Zynx industrial portfolio structure
- Portfolio tag target: 1.0.0-portfolio
