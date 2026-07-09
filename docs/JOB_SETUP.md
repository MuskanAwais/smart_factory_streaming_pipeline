# Databricks Job Setup — Fully Automated Pipeline

Schedule `07_auto_pipeline.py` to run automatically every 2 minutes. No manual uploads.

---

## What you need to do (one time, ~10 minutes)

I **cannot** log into your Databricks workspace. You do **not** need to share credentials.
Everything is already in this repo — you just import and click a few buttons.

| Step | Who | Time |
|---|---|---|
| 1. Create volume in Databricks UI | You | 2 min |
| 2. Import notebooks from GitHub | You | 3 min |
| 3. Run `00_setup.py` once | You | 2 min |
| 4. Run `07_auto_pipeline.py` once (test) | You | 2 min |
| 5. Create scheduled Job | You | 3 min |
| 6. Build dashboard | You | 10 min |

After that: **fully automatic** — no more manual steps.

---

## Step 1 — Create the volume (Databricks UI)

1. Open **Catalog** → **workspace** → **default**
2. Click **Create** → **Volume**
3. Name: `smart_factory`
4. Click **Create**

---

## Step 2 — Import notebooks

### Option A — Databricks Repos (recommended)

1. **Workspace** → **Repos** → **Add Repo**
2. Paste your GitHub URL: `https://github.com/<you>/smart-factory-streaming-pipeline`
3. Notebooks appear under `Repos/<user>/smart-factory-streaming-pipeline/notebooks/`

### Option B — Manual import

1. **Workspace** → **Import**
2. Import each file from `notebooks/` folder:
   - `00_config.py`
   - `00_setup.py`
   - `01_spark_basics.py`
   - `02_producer.py`
   - `03_bronze.py`
   - `04_silver.py`
   - `05_gold.py`
   - `06_run_pipeline.py`
   - `07_auto_pipeline.py`

---

## Step 3 — Run setup (once)

1. Open `00_setup.py`
2. Attach **Serverless** compute
3. **Run all cells**

Expected output:
```
All folders created.
bronze_events created (empty)
silver_events created (empty)
...
SETUP COMPLETE
```

---

## Step 4 — Test the automated pipeline (once)

1. Open `07_auto_pipeline.py`
2. **Run all cells**
3. Wait ~1–2 minutes

Expected output:
```
PRODUCER COMPLETE — ~300 events written
Bronze done — +~300
Silver done — +~285
Gold done — +~10 windows
AUTOMATED PIPELINE RUN COMPLETE
```

Verify in SQL:
```sql
SELECT * FROM workspace.default.pipeline_runs ORDER BY run_timestamp DESC LIMIT 5;
SELECT * FROM workspace.default.pipeline_health;
```

---

## Step 5 — Schedule the Job (automation)

1. Go to **Workflows** → **Jobs** → **Create job**
2. Configure:

| Setting | Value |
|---|---|
| **Job name** | `Smart Factory Auto Pipeline` |
| **Task type** | Notebook |
| **Notebook** | `07_auto_pipeline.py` |
| **Compute** | Serverless (or your cluster) |
| **Parameters** | `burst_seconds` = `30` (optional) |

3. **Schedule**:
   - Trigger type: **Scheduled**
   - Cron: `*/2 * * * *` (every 2 minutes)
   - Or: every 5 minutes if you want lower compute usage

4. Click **Create** → **Run now** to test the job

### What happens every 2 minutes

```text
Job triggers
  → Producer writes ~300 JSON files to landing/
  → Bronze ingests new files
  → Silver cleans and filters bad rows
  → Gold computes 1-minute window metrics
  → Run logged to pipeline_runs
  → Dashboard auto-refreshes
```

---

## Step 6 — Build the dashboard

Follow [`DASHBOARD_SETUP.md`](DASHBOARD_SETUP.md) to create 6 tiles.

Enable **Auto-refresh: 30 seconds** on the dashboard.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Path does not exist` | Run `00_setup.py` first |
| `Volume not found` | Create `smart_factory` volume in Catalog |
| Job fails on producer | Check volume permissions; re-run `00_setup.py` |
| Dashboard empty | Run `07_auto_pipeline.py` manually first |
| Views not found | Re-run `00_setup.py` (creates SQL views) |
| Bronze not growing | Check landing folder has files: `dbutils.fs.ls("/Volumes/.../landing")` |
| Job quota exceeded (Free Edition) | Increase interval to 5 minutes |

---

## Security note

**Do not share Databricks credentials in chat or commit them to GitHub.**

If you want CLI automation later, use Databricks personal access tokens stored in environment variables locally — never in the repo.

---

## Optional — adjust producer volume

In `07_auto_pipeline.py`, the `burst_seconds` widget controls how many events each job run generates:

| burst_seconds | Events per run (10 machines) | Approx. |
|---|---|---|
| 15 | ~150 | Light |
| 30 | ~300 | Default |
| 60 | ~600 | Heavy |

Change via Job task parameters: `{"burst_seconds": "30"}`
