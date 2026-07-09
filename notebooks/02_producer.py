# Databricks notebook source
# Module 2 — IoT Producer (writes directly to landing volume)
#
# No manual upload needed. JSON files are written to:
#   /Volumes/workspace/default/smart_factory/landing/

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("burst_seconds", str(PRODUCER_BURST_SECONDS), "Run duration (seconds)")
dbutils.widgets.text("machine_count", str(MACHINE_COUNT), "Number of machines")
dbutils.widgets.text("corrupt_rate", str(CORRUPT_RATE), "Corrupt event rate (0.0-1.0)")

burst_seconds = int(dbutils.widgets.get("burst_seconds"))
machine_count = int(dbutils.widgets.get("machine_count"))
corrupt_rate = float(dbutils.widgets.get("corrupt_rate"))

print(f"Producer config: {machine_count} machines, {burst_seconds}s burst, corrupt_rate={corrupt_rate}")
print(f"Output: {LANDING_PATH}")

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime, timezone

STATUSES = ("running", "idle", "error")
CORRUPTION_TYPES = (
    "null_machine_id", "high_temperature", "invalid_status",
    "bad_timestamp", "high_humidity", "high_vibration",
)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_machine_ids(count):
    width = max(2, len(str(count)))
    return [f"machine_{i:0{width}d}" for i in range(1, count + 1)]


def generate_valid_event(machine_id, timestamp=None):
    return {
        "machine_id": machine_id,
        "temperature": round(random.uniform(45.0, 80.0), 1),
        "humidity": round(random.uniform(30.0, 70.0), 1),
        "vibration": round(random.uniform(0.1, 5.0), 1),
        "status": random.choice(STATUSES),
        "timestamp": timestamp or utc_now_iso(),
    }


def corrupt_event(event, corruption_type=None):
    corrupted = dict(event)
    corruption = corruption_type or random.choice(CORRUPTION_TYPES)
    if corruption == "null_machine_id":
        corrupted["machine_id"] = None
    elif corruption == "high_temperature":
        corrupted["temperature"] = 999
    elif corruption == "invalid_status":
        corrupted["status"] = "melting"
    elif corruption == "bad_timestamp":
        corrupted["timestamp"] = "abc"
    elif corruption == "high_humidity":
        corrupted["humidity"] = 150
    elif corruption == "high_vibration":
        corrupted["vibration"] = 75
    return corrupted


def event_filename(event):
    machine_part = event.get("machine_id") or "unknown"
    safe_ts = str(event.get("timestamp", "no_timestamp")).replace(":", "-")
    return f"{machine_part}_{safe_ts}_{uuid.uuid4().hex[:8]}.json"


def write_event_to_volume(event):
    """Write one JSON file directly to the Unity Catalog landing volume."""
    path = f"{LANDING_PATH}/{event_filename(event)}"
    content = json.dumps(event, indent=2) + "\n"
    dbutils.fs.put(path, content, overwrite=False)
    return path


def count_landing_files():
    try:
        return len(dbutils.fs.ls(LANDING_PATH))
    except Exception:
        return 0


def run_producer_burst(machine_count, burst_seconds, corrupt_rate, events_per_second=1.0):
    machine_ids = build_machine_ids(machine_count)
    batch_interval = 1.0 / events_per_second
    events_written = 0
    corrupt_written = 0
    started = time.monotonic()

    while time.monotonic() - started < burst_seconds:
        batch_start = time.monotonic()
        for machine_id in machine_ids:
            event = generate_valid_event(machine_id)
            if corrupt_rate > 0 and random.random() < corrupt_rate:
                event = corrupt_event(event)
                corrupt_written += 1
            write_event_to_volume(event)
            events_written += 1

        elapsed = time.monotonic() - batch_start
        sleep_for = batch_interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

    return events_written, corrupt_written

# COMMAND ----------

files_before = count_landing_files()
print(f"Landing files before: {files_before}")

events_written, corrupt_written = run_producer_burst(
    machine_count=machine_count,
    burst_seconds=burst_seconds,
    corrupt_rate=corrupt_rate,
    events_per_second=EVENTS_PER_SECOND,
)

files_after = count_landing_files()
files_added = files_after - files_before

print()
print("=" * 60)
print("PRODUCER COMPLETE")
print("=" * 60)
print(f"  Events written : {events_written:,} ({corrupt_written:,} corrupt)")
print(f"  Landing files  : {files_after:,} total (+{files_added:,} new)")
print(f"  Location       : {LANDING_PATH}")
print("=" * 60)

# Return counts for orchestrator (07_auto_pipeline reads via dbutils.notebook.run return value)
dbutils.notebook.exit(json.dumps({
    "events_written": events_written,
    "corrupt_written": corrupt_written,
    "files_added": files_added,
    "landing_files_total": files_after,
}))
