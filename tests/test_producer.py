"""Unit tests for the IoT event producer."""

from __future__ import annotations

import json
import random
from datetime import datetime
from pathlib import Path

import pytest

from producer.generate_events import (
    CORRUPTION_TYPES,
    REQUIRED_FIELDS,
    build_event,
    build_machine_ids,
    corrupt_event,
    event_filename,
    generate_valid_event,
    run_producer,
    should_corrupt,
    utc_now_iso,
    write_event,
)


def test_build_machine_ids_default_count():
    machine_ids = build_machine_ids(10)
    assert machine_ids == [f"machine_{index:02d}" for index in range(1, 11)]


def test_build_machine_ids_single_machine():
    assert build_machine_ids(1) == ["machine_01"]


def test_generate_valid_event_has_all_required_fields():
    event = generate_valid_event("machine_03")
    assert set(event.keys()) == set(REQUIRED_FIELDS)


def test_generate_valid_event_field_ranges():
    event = generate_valid_event("machine_03")
    assert event["machine_id"] == "machine_03"
    assert -20 <= event["temperature"] <= 150
    assert 0 <= event["humidity"] <= 100
    assert 0 <= event["vibration"] <= 50
    assert event["status"] in {"running", "idle", "error"}


def test_timestamp_is_iso8601_utc():
    timestamp = utc_now_iso()
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    assert timestamp.endswith("Z")
    assert parsed.tzinfo is not None


def test_corrupt_event_applies_known_corruptions():
    base_event = generate_valid_event("machine_01", timestamp="2026-07-07T15:04:05Z")

    assert corrupt_event(base_event, "null_machine_id")["machine_id"] is None
    assert corrupt_event(base_event, "high_temperature")["temperature"] == 999
    assert corrupt_event(base_event, "invalid_status")["status"] == "melting"
    assert corrupt_event(base_event, "bad_timestamp")["timestamp"] == "abc"
    assert corrupt_event(base_event, "high_humidity")["humidity"] == 150
    assert corrupt_event(base_event, "high_vibration")["vibration"] == 75


def test_corrupt_event_supports_all_configured_types():
    base_event = generate_valid_event("machine_01")
    for corruption_type in CORRUPTION_TYPES:
        corrupted = corrupt_event(base_event, corruption_type)
        assert corrupted is not base_event


def test_should_corrupt_respects_rate():
    rng = random.Random(42)
    results = [should_corrupt(0.5, rng=rng) for _ in range(1000)]
    corrupt_count = sum(results)
    assert 350 <= corrupt_count <= 650


def test_should_corrupt_zero_rate_never_corrupts():
    rng = random.Random(7)
    assert all(not should_corrupt(0.0, rng=rng) for _ in range(100))


def test_build_event_corrupt_rate_is_respected():
    rng = random.Random(99)
    corrupt_count = 0
    total = 2000
    for _ in range(total):
        _, is_corrupt = build_event("machine_01", corrupt_rate=0.05, rng=rng)
        corrupt_count += int(is_corrupt)
    observed_rate = corrupt_count / total
    assert 0.03 <= observed_rate <= 0.07


def test_write_event_creates_valid_json_file(tmp_path: Path):
    event = generate_valid_event("machine_02", timestamp="2026-07-07T15:04:05Z")
    file_path = write_event(tmp_path, event)

    assert file_path.exists()
    assert file_path.suffix == ".json"
    loaded = json.loads(file_path.read_text(encoding="utf-8"))
    assert loaded == event


def test_event_filename_is_unique_per_call():
    event = generate_valid_event("machine_02", timestamp="2026-07-07T15:04:05Z")
    first = event_filename(event)
    second = event_filename(event)
    assert first != second


def test_run_producer_writes_expected_batch(tmp_path: Path):
    events_written = run_producer(
        machine_count=3,
        events_per_second=10.0,
        output_path=tmp_path,
        corrupt_rate=0.0,
        log_interval_seconds=1,
        max_runtime_seconds=0.2,
    )

    files = list(tmp_path.glob("*.json"))
    assert events_written >= 3
    assert len(files) == events_written

    for file_path in files:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        assert set(payload.keys()) == set(REQUIRED_FIELDS)


def test_run_producer_includes_corrupt_events(tmp_path: Path):
    rng_seed = 123
    random.seed(rng_seed)

    run_producer(
        machine_count=5,
        events_per_second=20.0,
        output_path=tmp_path,
        corrupt_rate=1.0,
        log_interval_seconds=1,
        max_runtime_seconds=0.1,
    )

    payloads = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in tmp_path.glob("*.json")
    ]
    assert payloads
    assert any(event.get("machine_id") is None for event in payloads)
    assert any(event.get("status") == "melting" for event in payloads)
    assert any(event.get("timestamp") == "abc" for event in payloads)
