# Project Context

Working memory for the **Smart Factory Streaming Pipeline**.
Use this file to resume work without re-explaining Module 1–3.

---

## 1. What this project is

Build a beginner-friendly **real-time IoT streaming Medallion pipeline** on Databricks:

`Producer JSON → Landing → Bronze → Silver → Gold → Dashboard`

| Layer | Job |
|---|---|
| Producer | Simulate machine sensor events |
| Landing | Raw JSON files |
| Bronze | Raw ingest (no cleaning) |
| Silver | Clean + validate |
| Gold | Windowed machine-health metrics |
| Dashboard | Live Databricks SQL views |

Source of truth:
- [`.planning/MASTER_PLAN.md`](.planning/MASTER_PLAN.md) — learning + build guide
- [`.planning/SPEC.md`](.planning/SPEC.md) — acceptance criteria (SPEC-driven)

---

## 2. Current progress

| Module | Status | Notes |
|---|---|---|
| 1 Project Setup | Done | Repo structure, `requirements.txt`, `.gitignore`, README |
| 2 IoT Producer | Done | JSON events + ~5% corrupt records + unit tests |
| 3 Databricks + Spark Basics | Done | Free Edition workspace + batch notebook |
| 4 Bronze Streaming | Done | `bronze_events` via `readStream` + `availableNow` trigger |
| 5 Silver Cleaning | Done | `silver_events` with validation, corrupt rows dropped |
| 6 Gold Analytics | Done | `gold_machine_metrics` windowed metrics + overheating flag |
| 7 Dashboard | Done | AI/BI dashboard over `gold_machine_metrics` (4 tiles) |
| 8 Testing + Docs | Next | Full pytest suite, README, screenshots |

Latest commits:
- `7c01e70` Initial project structure
- `63bfdb0` Add IoT event producer for Module 2
- `9f02b8a` Add Spark basics notebook for Module 3

---

## 3. Databricks environment

| Item | Value |
|---|---|
| Edition | Databricks Free Edition |
| Workspace | `https://dbc-c713f8a9-da8f.cloud.databricks.com/` |
| Login email | `muskan.awais@devsinc.com` |
| Volume | Managed: `smart_factory` |
| Catalog / schema | `workspace` / `default` |
| Landing path in DBX | `/Volumes/workspace/default/smart_factory/landing` |
| Local landing path | `data/landing/` (gitignored) |

Module 6 verified:
- Gold reads Silver as stream with 2-minute watermark + 1-minute tumbling windows
- Metrics: `avg_temperature`, `max_temperature`, `avg_vibration`, `event_count`, `error_count`
- `is_overheating` when `max_temperature > 85`
- Free Edition: `outputMode("append")` (Delta does not support `update` on serverless)

Module 7 verified:
- VIEW: `workspace.default.gold_machine_metrics` over Delta path
- AI/BI dashboard: 4 tiles (temp line, errors table, overheating table, vibration line)
- Auto-refresh enabled on published dashboard

Dashboard SQL queries: `docs/dashboard_queries.sql`

---

## 4. Event contract (REQ-DATA-1)

Every event has exactly these fields:

| Field | Type | Notes |
|---|---|---|
| `machine_id` | string | `machine_01` … `machine_10` |
| `temperature` | double | valid range -20 to 150 |
| `humidity` | double | valid range 0 to 100 |
| `vibration` | double | valid range 0 to 50 |
| `status` | string | `running` / `idle` / `error` |
| `timestamp` | string | ISO-8601 UTC |

Producer config defaults (`producer/config.py`):
- `MACHINE_COUNT = 10`
- `EVENTS_PER_SECOND = 1.0`
- `OUTPUT_PATH = data/landing`
- `CORRUPT_RATE = 0.05`

Corrupt examples intentionally include null `machine_id`, out-of-range values, invalid status, bad timestamp. Corrupt null-`machine_id` files may appear as `unknown_....json`.

---

## 5. Key local files

```text
producer/
  config.py
  generate_events.py

notebooks/
  01_spark_basics.py          # Module 3 (batch only)
  03_bronze.py                # Module 4 (Bronze streaming)
  04_silver.py                # Module 5 (Silver cleaning)
  05_gold.py                  # Module 6 (Gold windowed metrics)

tests/
  test_producer.py

.planning/
  MASTER_PLAN.md
  SPEC.md

data/landing/                 # local JSON output (gitignored)
```

Not created yet (intentional):
- Dashboard SQL queries / screenshots (Module 7)

---

## 6. How to run what exists

```powershell
cd D:\Engineering\Projects\week02\smart-factory-streaming-pipeline
.venv\Scripts\activate

# Producer (Ctrl+C to stop)
python -m producer.generate_events

# Producer tests
pytest tests/test_producer.py -v
```

In Databricks, open / import `notebooks/01_spark_basics.py` and keep:

`LANDING_PATH = "/Volumes/workspace/default/smart_factory/landing"`

---

## 7. Next module (Module 8 — Testing and Documentation)

Goal: portfolio-ready repo.

Tasks:
- `tests/test_validation.py` for REQ-DATA-2 rules
- Full `pytest` pass
- README with screenshots in `docs/screenshots/`
- Optional: push to GitHub

## 8. Working rules

1. Build **one module at a time** in SPEC order (M1 → M8).
2. Verify acceptance criteria before moving on.
3. Prefer SPEC language (`THE SYSTEM SHALL` / AC IDs) when checking done.
4. Keep v1 simple: no Kafka, DLT, Auto Loader, CI/CD, or Fabric.

---

*Last updated: 2026-07-08 — Module 7 complete; ready for Module 8.*
