# Databricks notebook source
# Shared configuration — %run this notebook from other pipeline notebooks
#
# Usage in another notebook:
#   %run ./00_config

# COMMAND ----------

VOLUME_BASE = "/Volumes/workspace/default/smart_factory"

LANDING_PATH = f"{VOLUME_BASE}/landing"
BRONZE_TABLE_PATH = f"{VOLUME_BASE}/tables/bronze_events"
SILVER_TABLE_PATH = f"{VOLUME_BASE}/tables/silver_events"
GOLD_TABLE_PATH = f"{VOLUME_BASE}/tables/gold_machine_metrics"
PIPELINE_RUNS_PATH = f"{VOLUME_BASE}/tables/pipeline_runs"
CHECKPOINT_BRONZE = f"{VOLUME_BASE}/checkpoints/bronze"
CHECKPOINT_SILVER = f"{VOLUME_BASE}/checkpoints/silver"
CHECKPOINT_GOLD = f"{VOLUME_BASE}/checkpoints/gold"

# Producer defaults (override via widgets in 02_producer / 07_auto_pipeline)
MACHINE_COUNT = 10
EVENTS_PER_SECOND = 1.0
CORRUPT_RATE = 0.05
PRODUCER_BURST_SECONDS = 30

VALID_STATUSES = ["running", "idle", "error"]

CATALOG = "workspace"
SCHEMA = "default"

print("Config loaded:")
print(f"  Volume base : {VOLUME_BASE}")
print(f"  Landing     : {LANDING_PATH}")
print(f"  Bronze      : {BRONZE_TABLE_PATH}")
print(f"  Silver      : {SILVER_TABLE_PATH}")
print(f"  Gold        : {GOLD_TABLE_PATH}")
print(f"  Pipeline log: {PIPELINE_RUNS_PATH}")
