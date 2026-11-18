# Architecture

## Context diagram

```mermaid
flowchart LR
  XLSX[OBT Excel on UC volume] --> BR[Bronze streaming tables]
  BR --> SC[Silver core SCD1/SCD2]
  SC --> SD[Silver dimensional MVs]
  SD --> GD[Gold aggregations]
  JOB[bronze_to_gold job] --> BR
  JOB --> SC
  JOB --> SD
  JOB --> GD
```

## Implementation flow

```mermaid
flowchart TB
  P1[obt_bronze] --> P2[obt_silver_core]
  P2 --> P3[obt_silver_dimensional]
  P3 --> P4[obt_gold]
```

## Design notes

- Prefer layered medallion / governed platform patterns where applicable.
- Keep configuration and secrets outside Git.
- Preserve auditability: documentation must match repository evidence.
