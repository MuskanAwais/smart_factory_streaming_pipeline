"""Unit tests for REQ-DATA-2 validation rules."""

from __future__ import annotations

import pytest

from producer.validation import is_valid_event


def _valid_event() -> dict:
    return {
        "machine_id": "machine_01",
        "temperature": 72.4,
        "humidity": 45.1,
        "vibration": 2.3,
        "status": "running",
        "timestamp": "2026-07-07T15:04:05Z",
    }


def test_valid_event_passes_all_rules():
    assert is_valid_event(_valid_event()) is True


def test_null_machine_id_fails_rule_1():
    event = _valid_event()
    event["machine_id"] = None
    assert is_valid_event(event) is False


def test_high_temperature_fails_rule_2():
    event = _valid_event()
    event["temperature"] = 999
    assert is_valid_event(event) is False


def test_low_temperature_fails_rule_2():
    event = _valid_event()
    event["temperature"] = -21
    assert is_valid_event(event) is False


def test_high_humidity_fails_rule_3():
    event = _valid_event()
    event["humidity"] = 150
    assert is_valid_event(event) is False


def test_negative_humidity_fails_rule_3():
    event = _valid_event()
    event["humidity"] = -1
    assert is_valid_event(event) is False


def test_high_vibration_fails_rule_4():
    event = _valid_event()
    event["vibration"] = 75
    assert is_valid_event(event) is False


def test_invalid_status_fails_rule_5():
    event = _valid_event()
    event["status"] = "melting"
    assert is_valid_event(event) is False


def test_bad_timestamp_fails_rule_6():
    event = _valid_event()
    event["timestamp"] = "abc"
    assert is_valid_event(event) is False


def test_null_timestamp_fails_rule_6():
    event = _valid_event()
    event["timestamp"] = None
    assert is_valid_event(event) is False


@pytest.mark.parametrize("status", ["running", "idle", "error"])
def test_valid_statuses_pass(status: str):
    event = _valid_event()
    event["status"] = status
    assert is_valid_event(event) is True
