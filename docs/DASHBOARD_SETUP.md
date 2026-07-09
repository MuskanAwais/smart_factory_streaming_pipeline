# Dashboard Setup Guide — Automated Pipeline

Build the **Smart Factory Pipeline Monitor** dashboard with **6 tiles** that auto-update as the scheduled job runs.

## Prerequisites

1. Volume `smart_factory` created in Catalog
2. `00_setup.py` run once (creates views + empty tables)
3. `07_auto_pipeline.py` run at least once (or Job scheduled)
4. Dashboard **auto-refresh** enabled (30 seconds)

---

## Dashboard Layout

```text
┌──────────────────────────────────────────────────────────────────────────┐
│  Smart Factory Pipeline Monitor                       [Auto-refresh 30s] │
├──────────────────────────────────────────────────────────────────────────┤
│  TILE 1: Pipeline Flow (Table)                                           │
│  Landing → Bronze → Silver → Gold → Rejected  (where data lives)         │
├──────────────────────────────┬───────────────────────────────────────────┤
│  TILE 2: Quality Funnel      │  TILE 6: Pipeline Run History             │
│  (Bar Chart)                 │  (Line Chart — rows growing over time)    │
├──────────────────────────────┴───────────────────────────────────────────┤
│  TILE 3: Ingestion Activity (Line Chart — events per minute)             │
├──────────────────────────────┬───────────────────────────────────────────┤
│  TILE 4: Temperature Trend   │  TILE 5: Health Alerts (Table)            │
│  (Line Chart)                │  Overheating + Errors                     │
└──────────────────────────────┴───────────────────────────────────────────┘
```

---

## Step 1 — Verify views exist

Run in Databricks SQL:

```sql
SELECT * FROM workspace.default.pipeline_health;
SELECT * FROM workspace.default.pipeline_runs ORDER BY run_timestamp DESC LIMIT 5;
```

If views are missing, re-run `00_setup.py`.

---

## Step 2 — Create dashboard

1. **Dashboards** → **Create dashboard**
2. Name: **Smart Factory Pipeline Monitor**
3. Enable **Auto-refresh: 30 seconds**

---

## Step 3 — Add 6 tiles

Copy queries from [`dashboard_queries.sql`](dashboard_queries.sql).

### Tile 1 — Pipeline Flow (Table)

| Setting | Value |
|---|---|
| Query | ARTIFACT 1 (use the `USE THIS QUERY FOR TILE 1` block) |
| Visualization | **Table** |
| Columns | `stage`, `location`, `row_count`, `last_updated`, `what_happens` |

**What it shows:** Data path through the pipeline — where data is saved and what happens at each layer. Updates every job run.

---

### Tile 2 — Data Quality Funnel (Bar Chart)

| Setting | Value |
|---|---|
| Query | ARTIFACT 2 |
| Visualization | **Bar chart** |
| X-axis | `stage` |
| Y-axis | `event_count` |

**What it shows:** How many events are raw vs valid vs rejected by Silver.

---

### Tile 3 — Ingestion Activity (Line Chart)

| Setting | Value |
|---|---|
| Query | ARTIFACT 3 |
| Visualization | **Line chart** |
| X-axis | `minute` |
| Y-axis | `event_count` |

**What it shows:** Events arriving per minute — proves live data flow.

---

### Tile 4 — Temperature Trend (Line Chart)

| Setting | Value |
|---|---|
| Query | ARTIFACT 4 |
| Visualization | **Line chart** |
| X-axis | `window_start` |
| Y-axis | `avg_temperature` |
| Series | `machine_id` |

**What it shows:** Per-machine temperature from Gold layer.

---

### Tile 5 — Health Alerts (Table)

| Setting | Value |
|---|---|
| Query | ARTIFACT 5 |
| Visualization | **Table** |

**What it shows:** Machines overheating or in error state.

---

### Tile 6 — Pipeline Run History (Line Chart)

| Setting | Value |
|---|---|
| Query | ARTIFACT 6 |
| Visualization | **Line chart** |
| X-axis | `run_timestamp` |
| Y-axis | `bronze`, `silver`, `gold` (add as separate series) |

**What it shows:** Row counts growing over time as the automated job runs. This is the "data moves here → here → here" chart.

---

## How it auto-updates

```text
Every 2 min: Job runs 07_auto_pipeline.py
  → New rows in Bronze, Silver, Gold
  → New row in pipeline_runs
  → Dashboard refreshes (30s)
  → All 6 tiles show new data
```

No manual upload. No manual notebook runs (after Job is scheduled).

---

## Screenshot

Save full dashboard as `docs/screenshots/dashboard.png`.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| All tiles empty | Run `07_auto_pipeline.py` manually once |
| Tile 1 row_count = 0 | Job hasn't run yet |
| Tile 6 empty | `pipeline_runs` table empty — run automated pipeline |
| Tiles don't update | Check Job is running; enable dashboard auto-refresh |
| Views error | Re-run `00_setup.py` |
