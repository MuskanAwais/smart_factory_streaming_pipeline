"""Validation rules for IoT events (REQ-DATA-2). Used by Silver layer tests."""

from __future__ import annotations

from datetime import datetime
from typing import Any

VALID_STATUSES = frozenset({"running", "idle", "error"})


def is_valid_timestamp(timestamp: str | None) -> bool:
    """Return True if timestamp parses as a valid datetime."""
    if timestamp is None:
        return False
    try:
        normalized = timestamp.replace("Z", "+00:00")
        datetime.fromisoformat(normalized)
        return True
    except (TypeError, ValueError):
        return False


def is_valid_event(event: dict[str, Any]) -> bool:
    """Return True only if the event passes all REQ-DATA-2 rules."""
    machine_id = event.get("machine_id")
    temperature = event.get("temperature")
    humidity = event.get("humidity")
    vibration = event.get("vibration")
    status = event.get("status")
    timestamp = event.get("timestamp")

    if machine_id is None:
        return False

    if temperature is None or not (-20 <= float(temperature) <= 150):
        return False

    if humidity is None or not (0 <= float(humidity) <= 100):
        return False

    if vibration is None or not (0 <= float(vibration) <= 50):
        return False

    if status not in VALID_STATUSES:
        return False

    if not is_valid_timestamp(timestamp):
        return False

    return True
