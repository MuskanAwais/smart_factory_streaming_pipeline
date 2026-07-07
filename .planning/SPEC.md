# SPEC — Spec-Driven Requirements

# Real-Time IoT Streaming Medallion Pipeline

| Field | Value |
|---|---|
| **Version** | 1.0 |
| **Date** | 2026-07-07 |
| **Method** | Spec-Driven Development |
| **Build guide** | [MASTER_PLAN.md](MASTER_PLAN.md) |

---

## How to read this spec

This document defines **what the system must do**, module by module, using a spec-driven format. Every requirement is written as:

- **User Story** — who wants it and why.
- **Acceptance Criteria** — testable rules in EARS notation:
  - `WHEN <trigger> THE SYSTEM SHALL <behavior>` (event-driven)
  - `IF <condition> THEN THE SYSTEM SHALL <behavior>` (conditional)
  - `THE SYSTEM SHALL <behavior>` (always true)

**Rule:** A module is "done" only when every acceptance criterion is verifiably true. Build modules in order (M1 → M8).

---

## Glossary

| Term | Meaning |
|---|---|
| Event | One IoT sensor reading (6 fields) |
| Landing folder | Directory where the producer writes JSON files |
| Bronze | Raw Delta table (unchanged events) |
| Silver | Cleaned, validated Delta table |
| Gold | Aggregated business-metrics Delta table |
| Valid event | Event passing all validation rules (REQ-DATA-2) |
| Corrupt event | Intentionally malformed event for testing |

---

## Shared Data Contract

Referenced by multiple modules.

### REQ-DATA-1 — Event Schema

THE SYSTEM SHALL represent every IoT event with exactly these fields:

| Field | Type | Example | Constraint |
|---|---|---|---|
| `machine_id` | string | `"machine_03"` | Pattern `machine_\d+` |
| `temperature` | double | `72.4` | -20 to 150 |
| `humidity` | double | `45.1` | 0 to 100 |
| `vibration` | double | `2.3` | 0 to 50 |
| `status` | string | `"running"` | `running` / `idle` / `error` |
| `timestamp` | string | `"2026-07-07T15:04:05Z"` | ISO-8601 UTC |

### REQ-DATA-2 — Validation Rules

An event is **valid** only if ALL hold:

1. `machine_id` is not null.
2. `temperature` is between -20 and 150.
3. `humidity` is between 0 and 100.
4. `vibration` is between 0 and 50.
5. `status` is one of `running`, `idle`, `error`.
6. `timestamp` parses to a valid datetime.

---

## Module 1 — Project Setup

### User Story
As a developer, I want a clean repository and environment, so that I can build and version the project reliably.

### Acceptance Criteria

- **AC-1.1** — THE SYSTEM SHALL provide the folder structure: `producer/`, `notebooks/`, `data/landing/`, `tests/`, `docs/`, `.planning/`.
- **AC-1.2** — THE SYSTEM SHALL include `requirements.txt`, `.gitignore`, and `README.md` at the repo root.
- **AC-1.3** — THE `.gitignore` SHALL exclude `.venv/`, `data/`, and `__pycache__/`.
- **AC-1.4** — WHEN a developer runs `pip install -r requirements.txt` THE SYSTEM SHALL install without errors.
- **AC-1.5** — WHEN the first commit is pushed THE repository SHALL appear on GitHub with the correct structure.

### Definition of Done
Repo structure exists, dependencies install, and initial commit is on GitHub.

---

## Module 2 — Python IoT Event Generator

### User Story
As a data engineer, I want a script that simulates factory machines, so that I have a continuous stream of test events.

### Acceptance Criteria

- **AC-2.1** — THE SYSTEM SHALL generate events conforming to REQ-DATA-1.
- **AC-2.2** — WHEN the producer runs THE SYSTEM SHALL emit one event per machine per second (configurable rate).
- **AC-2.3** — THE SYSTEM SHALL support a configurable machine count (default 10) producing IDs `machine_01`..`machine_10`.
- **AC-2.4** — THE SYSTEM SHALL write each event as a JSON file into the landing folder (`data/landing/`).
- **AC-2.5** — THE `timestamp` field SHALL be the current UTC time in ISO-8601 format.
- **AC-2.6** — WHEN `corrupt_rate` is configured THE SYSTEM SHALL inject corrupt events (null fields, out-of-range values, invalid status) at approximately that rate.
- **AC-2.7** — WHEN the user presses Ctrl+C THE SYSTEM SHALL stop gracefully without corrupting the last file.
- **AC-2.8** — THE SYSTEM SHALL log the number of events written at a regular interval.

### Configuration Contract (`producer/config.py`)

| Setting | Default |
|---|---|
| `MACHINE_COUNT` | 10 |
| `EVENTS_PER_SECOND` | 1.0 |
| `OUTPUT_PATH` | `data/landing/` |
| `CORRUPT_RATE` | 0.05 |

### Definition of Done
Producer runs for 10s and produces ~100 valid JSON events plus ~5% corrupt ones, and stops cleanly.

---

## Module 3 — Databricks Setup and Spark Basics

### User Story
As a beginner, I want Databricks configured and basic PySpark proven, so that I am ready to build streaming layers.

### Acceptance Criteria

- **AC-3.1** — THE SYSTEM SHALL provide an accessible Databricks Free Edition workspace.
- **AC-3.2** — WHEN `spark.range(5).show()` is run THE notebook SHALL display 5 rows.
- **AC-3.3** — THE SYSTEM SHALL define the event schema explicitly using `StructType` (schema inference SHALL NOT be used).
- **AC-3.4** — WHEN sample JSON is loaded THE notebook SHALL read it into a DataFrame using the explicit schema.
- **AC-3.5** — THE notebook SHALL demonstrate `select`, `filter`, `withColumn`, and `groupBy().agg()`.
- **AC-3.6** — WHEN `groupBy("machine_id").count()` runs THE result SHALL contain one row per machine.

### Definition of Done
`notebooks/01_spark_basics.py` runs end-to-end and reads sample data with an explicit schema.

---

## Module 4 — Bronze Streaming Layer

### User Story
As a data engineer, I want raw events ingested into a Bronze table, so that I have a complete, unchanged record of everything received.

### Acceptance Criteria

- **AC-4.1** — WHEN new JSON files appear in the landing folder THE SYSTEM SHALL read them as a stream (`readStream`).
- **AC-4.2** — THE SYSTEM SHALL write all events to the `bronze_events` Delta table in append mode.
- **AC-4.3** — THE SYSTEM SHALL NOT validate, filter, or transform events in the Bronze layer.
- **AC-4.4** — THE SYSTEM SHALL use a checkpoint location so the stream can resume after restart.
- **AC-4.5** — WHEN the producer is running THE `bronze_events` row count SHALL increase over time.
- **AC-4.6** — IF the stream is stopped and restarted THEN THE SYSTEM SHALL resume without losing or duplicating events.
- **AC-4.7** — Corrupt events SHALL still be stored in Bronze (nothing is dropped).

### Streaming Contract

| Setting | Value |
|---|---|
| Output mode | append |
| Trigger | `processingTime='10 seconds'` |
| Checkpoint | `{CHECKPOINT_BASE}/bronze` |

### Definition of Done
`bronze_events` grows as the producer runs, includes corrupt rows, and survives a restart.

---

## Module 5 — Silver Cleaning Layer

### User Story
As a data engineer, I want cleaned and validated data in Silver, so that downstream analytics only use trustworthy events.

### Acceptance Criteria

- **AC-5.1** — WHEN new rows arrive in `bronze_events` THE SYSTEM SHALL read them as a stream.
- **AC-5.2** — THE SYSTEM SHALL cast `temperature`, `humidity`, and `vibration` to double.
- **AC-5.3** — THE SYSTEM SHALL parse `timestamp` into an `event_time` column of type timestamp.
- **AC-5.4** — THE SYSTEM SHALL normalize `status` to lowercase.
- **AC-5.5** — IF an event fails any rule in REQ-DATA-2 THEN THE SYSTEM SHALL exclude it from `silver_events`.
- **AC-5.6** — THE `silver_events` table SHALL contain no nulls in `machine_id`, `temperature`, `humidity`, `vibration`, `status`, or `event_time`.
- **AC-5.7** — WHEN corrupt producer events are ingested THE SYSTEM SHALL ensure they do NOT appear in `silver_events`.
- **AC-5.8** — THE `silver_events` row count SHALL be less than the `bronze_events` count (bad rows removed).

### Streaming Contract

| Setting | Value |
|---|---|
| Source | `bronze_events` |
| Output mode | append |
| Trigger | `processingTime='10 seconds'` |
| Checkpoint | `{CHECKPOINT_BASE}/silver` |

### Definition of Done
`silver_events` contains only valid, typed rows with a real `event_time` and no corrupt records.

---

## Module 6 — Gold Analytics Layer

### User Story
As an analyst, I want per-machine metrics over time windows, so that I can monitor machine health in near real time.

### Acceptance Criteria

- **AC-6.1** — WHEN new rows arrive in `silver_events` THE SYSTEM SHALL read them as a stream.
- **AC-6.2** — THE SYSTEM SHALL group events by `machine_id` and a 1-minute tumbling window on `event_time`.
- **AC-6.3** — THE SYSTEM SHALL apply a 2-minute watermark on `event_time`.
- **AC-6.4** — THE SYSTEM SHALL compute `avg_temperature`, `max_temperature`, `avg_vibration`, `event_count`, and `error_count` per group.
- **AC-6.5** — THE SYSTEM SHALL set `is_overheating = true` WHEN `max_temperature > 85`.
- **AC-6.6** — THE SYSTEM SHALL write results to `gold_machine_metrics` with `window_start` and `window_end` columns.
- **AC-6.7** — WHEN error-status events arrive THE `error_count` SHALL increase for the affected machine/window.
- **AC-6.8** — IF an event arrives later than the watermark THEN THE SYSTEM SHALL drop it from aggregation.

### Streaming Contract

| Setting | Value |
|---|---|
| Source | `silver_events` |
| Output mode | update |
| Trigger | `processingTime='10 seconds'` |
| Watermark | 2 minutes on `event_time` |
| Window | 1-minute tumbling |
| Checkpoint | `{CHECKPOINT_BASE}/gold` |

### Definition of Done
`gold_machine_metrics` has one row per machine per minute with correct metrics and overheating flags.

---

## Module 7 — Dashboard

### User Story
As a factory operator, I want a live dashboard, so that I can see machine health and alerts at a glance.

### Acceptance Criteria

- **AC-7.1** — THE SYSTEM SHALL build the dashboard in Databricks SQL over `gold_machine_metrics`.
- **AC-7.2** — THE dashboard SHALL include a line chart of average temperature per machine over time.
- **AC-7.3** — THE dashboard SHALL include a tile listing machines currently in error.
- **AC-7.4** — THE dashboard SHALL include a tile of overheating alerts WHERE `is_overheating = true`.
- **AC-7.5** — THE dashboard SHALL include a line chart of vibration trend per machine.
- **AC-7.6** — WHEN the pipeline is running THE dashboard SHALL auto-refresh with new data.

### Definition of Done
All four tiles render and update live while the pipeline runs.

---

## Module 8 — Testing and Documentation

### User Story
As a job seeker, I want tests and documentation, so that the project is portfolio- and interview-ready.

### Acceptance Criteria

- **AC-8.1** — THE SYSTEM SHALL include producer unit tests verifying all 6 fields exist and the corrupt rate is respected.
- **AC-8.2** — THE SYSTEM SHALL include validation tests for each rule in REQ-DATA-2 (valid passes; each invalid case fails).
- **AC-8.3** — WHEN `pytest` is run THE test suite SHALL pass.
- **AC-8.4** — THE `README.md` SHALL include problem statement, architecture diagram, tech stack, run instructions, and screenshots.
- **AC-8.5** — THE `docs/screenshots/` folder SHALL contain images of Bronze, Silver, Gold tables and the dashboard.
- **AC-8.6** — THE SYSTEM SHALL have all code committed and pushed to GitHub.

### Test Matrix

| Test | Input | Expected |
|---|---|---|
| Valid event | Complete valid event | Passes all rules |
| Null machine_id | `machine_id = null` | Fails rule 1 |
| High temperature | `temperature = 999` | Fails rule 2 |
| Invalid status | `status = "melting"` | Fails rule 5 |
| Bad timestamp | `timestamp = "abc"` | Fails rule 6 |

### Definition of Done
`pytest` passes, README is complete, screenshots exist, and everything is on GitHub.

---

## Non-Functional Requirements

- **NFR-1 (Simplicity)** — THE SYSTEM SHALL run on Databricks Free Edition without paid features.
- **NFR-2 (Fault tolerance)** — THE SYSTEM SHALL use checkpoints so every streaming layer can restart safely.
- **NFR-3 (Reproducibility)** — THE SYSTEM SHALL be runnable by following the README alone.
- **NFR-4 (Scope)** — THE SYSTEM SHALL NOT include Kafka, DLT, Unity Catalog, Auto Loader, CI/CD, or Fabric in v1.

---

## Out of Scope (Future Specs)

These are explicitly NOT part of this spec:

- Kafka / message broker ingestion
- Databricks Lakeflow Declarative Pipelines (DLT)
- Quarantine / dead-letter table for bad rows
- Monitoring and alerting (email/Slack)
- CI/CD with GitHub Actions
- Microsoft Fabric (Eventstream + KQL) comparison

---

*Spec-driven: implement to satisfy every acceptance criterion. See [MASTER_PLAN.md](MASTER_PLAN.md) for the learning walkthrough.*
