# FAERS Pharma ETL Pipeline

![Python](https://img.shields.io/badge/Python-3-blue)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-lightgrey)
![Status](https://img.shields.io/badge/Status-In%20Progress-yellow)

End-to-end ETL pipeline built with PySpark and the Medallion Architecture (Bronze → Silver → Gold) on Databricks, transforming FDA Adverse Event Reporting System (FAERS) data and Clinical Trials data into analytics-ready tables for drug safety analysis.

## Business Problem

Every quarter, the FDA publishes hundreds of thousands of raw adverse event reports submitted by manufacturers, healthcare professionals, and consumers. In raw form, this data is inconsistent — mixed date formats, inconsistent units (weight, age, dosage), missing identifiers, and no structure for answering direct safety questions. Drug safety teams need a reliable way to turn this raw feed into trustworthy, queryable answers about which drugs carry the highest risk and which patients are most vulnerable.

## Project Objectives

- Build a reliable, reproducible pipeline that turns raw FAERS + Clinical Trials data into analytics-ready tables.
- Apply medallion architecture best practices — cleaning and standardizing progressively rather than transforming raw data directly into business logic.
- Answer concrete drug safety business questions (see Gold Layer below) rather than just storing clean data.

## Overview

This project ingests raw FAERS adverse event reports and clinical trial records, cleans and standardizes them, and builds business-ready Gold layer tables that answer real drug safety and patient risk questions — the kind used by drug safety officers, regulatory affairs teams, clinical researchers, and insurance companies.

**Scope:** Q4 FAERS data (single quarter).

## Architecture

```
Raw FAERS files (.txt, $ delimited) ─┐
Raw Clinical Trials file (.csv)      ─┤
                                      ▼
                              BRONZE LAYER
                    (raw ingestion → Parquet → cleaning,
                     deduplication, date standardization)
                                      │
                                      ▼
                              SILVER LAYER
                  (business standardization: unit conversions,
                   demographic bucketing, null handling,
                   consistent primary/case ID resolution)
                                      │
                                      ▼
                               GOLD LAYER
                    (analytics tables answering specific
                          business questions)
```

Each layer reads from the previous layer's Parquet output and writes its own cleaned/transformed Parquet output, following standard medallion architecture principles — no layer is skipped or bypassed.

## Repository Structure

```
├── bronze/       # Phase 1–3: raw ingestion, Parquet conversion, cleaning & standardization
├── silver/       # Phase 4: business-level transformation (unit conversion, ID resolution, bucketing)
├── gold/         # Phase 5: analytics tables per business scenario
└── config.py     # Centralized file paths, dedup keys, and pipeline configuration
```

## Data Sources

- **FAERS (FDA Adverse Event Reporting System)** — demographic (`demo`), drug (`drug`), indication (`indi`), reaction (`reac`), outcome (`outc`), and therapy (`ther`) files.
- **Clinical Trials (CT)** — trial metadata including status, phase, and start/completion dates.

## Data Model — FAERS Tables

| Table | Grain | Description |
|---|---|---|
| `demo` | One row per report (`primaryid`) | Patient demographics — age, sex, weight, reporter/occurrence country, report type |
| `drug` | One row per drug per report | Every drug named in a report, with route, dosage, dechallenge/rechallenge response |
| `indi` | One row per indication per report | The medical reason each drug was administered |
| `reac` | One row per reaction per report | Adverse reactions reported, plus any action taken with the drug |
| `outc` | One row per outcome per report | Outcome codes (death, hospitalization, disability, etc.) |
| `ther` | One row per therapy episode per report | Start/end dates and duration of drug therapy |

All six tables share `primaryid` (report identifier) and `caseid` (case identifier, may repeat across `primaryid` versions) as join keys — resolved consistently to `primary_id`/`case_id` in the Silver layer.

## Pipeline Details

### Bronze Layer
- Reads raw FAERS and CT files, converts to Parquet.
- Deduplicates records using configurable dedup keys per file.
- Drops records with a null `primaryid`.
- Standardizes inconsistent date formats (4/6/8-digit FAERS dates; multiple CT date formats) into proper date types.

### Silver Layer
- Resolves a consistent, non-null `case_id` across all six FAERS tables (falls back to `primaryid` when `caseid` is missing), ensuring reliable joins downstream.
- Standardizes patient age into years across all reported units (YR, MON, WK, DY, HR, DEC) and buckets patients into age groups (Neonate, Infant, Child, Teen, Adult, Elderly).
- Standardizes patient weight into kilograms (KG/LBS), with unit and value kept consistent — both null together when the unit is unknown.
- Normalizes drug-level fields (route, dosage, dechallenge/rechallenge response, lot number, NDA number) with explicit `UNKNOWN`/`NOT_REPORTED` handling instead of silent nulls.
- Categorizes outcome codes into readable labels (e.g., `DE` → `DEATH`, `HO` → `HOSPITALIZATION`).
- Standardizes therapy duration into days across all reported units.
- Cleans CT status and phase fields, handling inconsistent casing, underscores, and spacing.

### Gold Layer

| Scenario | Business Question | Used By | Status |
|---|---|---|---|
| **Drug Safety Scorecard** | Which drugs have the highest adverse event rates, and what are the most dangerous reactions associated with them? | Drug safety officers, regulatory affairs teams | Built |
| **Patient Risk Profile** | Which patient demographics (age, gender, weight) are most vulnerable to adverse events for a given drug? | Clinical research teams, doctors, insurance companies | Built |
| **Trial vs Real-World Comparison** | How do real-world adverse events compare against clinical trial data for the same drugs? | Regulatory affairs, clinical research | Planned |
| *Additional scenarios* | — | — | Planned |

**Drug Safety Scorecard** — ranks drugs by a tiered safety signal: fatal outcomes first, hospitalization count as tie-breaker, total adverse event volume as final tie-breaker. Surfaces each drug's most commonly reported reaction.

**Patient Risk Profile** — buckets patients by age group, gender, and weight range, then computes a proportional-reporting-style `risk_index` (a drug's adverse events within a demographic group relative to that group's total events across all drugs), the most common reaction, and worst observed outcome severity per drug-demographic combination.

### Sample Output — `drug_safety_scorecard`

| drug_name | total_adverse_events | unique_reaction_types | fatal_outcome_count | hospitalization_count | most_common_reaction | safety_score_rank |
|---|---|---|---|---|---|---|
| DRUG_A | 1,204 | 87 | 42 | 310 | Nausea | 1 |
| DRUG_B | 956 | 64 | 18 | 275 | Headache | 2 |
| DRUG_C | 843 | 71 | 9 | 190 | Dizziness | 3 |

*(illustrative — replace with a real sample from your Gold output once finalized)*

## Key Engineering Concepts Demonstrated

- **Medallion architecture** — progressive refinement across Bronze (raw), Silver (business-standardized), and Gold (analytics-ready) layers.
- **Window functions** — `row_number()` and `dense_rank()` for computing "most common reaction" and safety ranking per group.
- **Fan-out-safe aggregation** — deduplicating on the join grain before aggregating, so a one-to-many join (e.g., one report → many reactions) doesn't inflate downstream counts.
- **Consistent surrogate key resolution** — deriving a single, non-null `case_id` across all Silver tables to guarantee reliable joins.
- **Proportional signal detection** — a simplified proportional-reporting-style `risk_index` comparing a drug's events within a demographic group against that group's baseline across all drugs.
- **Explicit null semantics** — distinguishing "unknown/not reported" from silently dropped or defaulted values throughout Silver.

## Assumptions & Limitations

- Scoped to a single quarter (Q4) of FAERS data — not a multi-quarter trend analysis.
- `wt_cod` in this dataset only contained `KG`, `LBS`, and `NULL` — no `GMS` values were observed, so gram-to-kilogram conversion was not required for this quarter's data.
- Outcome severity tiering in the Patient Risk Profile groups several FAERS outcome codes (`LIFE THREATENING`, `DISABILITY`, `REQUIRED INTERVENTION`, `CONGENITAL ANOMALY`, `OTHER SERIOUS`) into a single "Hospitalised" severity tier — a simplification for ranking purposes, not a clinical judgment.
- No automated data quality test suite yet — validation was done via ad hoc row counts and distinct-value checks during development.

## Tech Stack

- **PySpark** — distributed data processing
- **Databricks** (Free Edition) — development and execution environment
- **Parquet** — storage format across all layers
- **Planned:** AWS S3 as a landing zone for cloud deployment

## Running the Pipeline

Each phase is designed to run as a Databricks notebook, in order:

1. Run the Bronze notebook — ingests raw files and produces cleaned Parquet tables.
2. Run the Silver notebook — reads Bronze output, applies business transformations.
3. Run the Gold notebook(s) — reads Silver output, produces analytics tables per scenario.

`config.py` centralizes file paths and dedup keys so the pipeline can be pointed at a new data drop without touching the ETL logic.

## Roadmap

- Complete remaining Gold layer business scenarios.
- Deploy pipeline to AWS, using S3 as a landing zone.

## License

MIT — see `LICENSE` for details.
