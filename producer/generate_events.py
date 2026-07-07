"""Simulate IoT sensor events for the smart factory streaming pipeline."""

from __future__ import annotations

import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from producer import config

logger = logging.getLogger(__name__)

STATUSES = ("running", "idle", "error")
REQUIRED_FIELDS = (
    "machine_id",
    "temperature",
    "humidity",
    "vibration",
    "status",
    "timestamp",
)

CORRUPTION_TYPES = (
    "null_machine_id",
    "high_temperature",
    "invalid_status",
    "bad_timestamp",
    "high_humidity",
    "high_vibration",
)

_shutdown_requested = False


def build_machine_ids(machine_count: int) -> list[str]:
    """Return machine IDs in the form machine_01 .. machine_NN."""
    if machine_count < 1:
        raise ValueError("machine_count must be at least 1")
    width = max(2, len(str(machine_count)))
    return [f"machine_{index:0{width}d}" for index in range(1, machine_count + 1)]


def utc_now_iso() -> str:
    """Return the current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def generate_valid_event(machine_id: str, timestamp: str | None = None) -> dict[str, Any]:
    """Generate a valid IoT event for one machine."""
    return {
        "machine_id": machine_id,
        "temperature": round(random.uniform(45.0, 80.0), 1),
        "humidity": round(random.uniform(30.0, 70.0), 1),
        "vibration": round(random.uniform(0.1, 5.0), 1),
        "status": random.choice(STATUSES),
        "timestamp": timestamp or utc_now_iso(),
    }


def corrupt_event(event: dict[str, Any], corruption_type: str | None = None) -> dict[str, Any]:
    """Return a copy of the event with one intentional corruption."""
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
    else:
        raise ValueError(f"Unknown corruption type: {corruption}")

    return corrupted


def should_corrupt(corrupt_rate: float, rng: random.Random | None = None) -> bool:
    """Decide whether the next event should be corrupted."""
    if corrupt_rate <= 0:
        return False
    if corrupt_rate >= 1:
        return True
    random_source = rng or random
    return random_source.random() < corrupt_rate


def build_event(
    machine_id: str,
    corrupt_rate: float,
    rng: random.Random | None = None,
    timestamp: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Generate a valid or corrupt event based on corrupt_rate."""
    event = generate_valid_event(machine_id, timestamp=timestamp)
    if should_corrupt(corrupt_rate, rng=rng):
        return corrupt_event(event), True
    return event, False


def event_filename(event: dict[str, Any]) -> str:
    """Build a unique JSON filename for an event."""
    machine_part = event.get("machine_id") or "unknown"
    timestamp_part = event.get("timestamp", "no_timestamp")
    safe_timestamp = str(timestamp_part).replace(":", "-")
    return f"{machine_part}_{safe_timestamp}_{uuid.uuid4().hex[:8]}.json"


def write_event(output_path: Path, event: dict[str, Any]) -> Path:
    """Write one event as a JSON file and return the file path."""
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / event_filename(event)
    with file_path.open("w", encoding="utf-8") as handle:
        json.dump(event, handle, indent=2)
        handle.write("\n")
    return file_path


def _handle_shutdown(signum: int, _frame: object | None) -> None:
    global _shutdown_requested
    signal_name = signal.Signals(signum).name
    logger.info("Received %s, shutting down gracefully...", signal_name)
    _shutdown_requested = True


def run_producer(
    machine_count: int = config.MACHINE_COUNT,
    events_per_second: float = config.EVENTS_PER_SECOND,
    output_path: Path = config.OUTPUT_PATH,
    corrupt_rate: float = config.CORRUPT_RATE,
    log_interval_seconds: int = config.LOG_INTERVAL_SECONDS,
    max_runtime_seconds: float | None = None,
) -> int:
    """Run the event producer until interrupted or max_runtime_seconds elapses."""
    global _shutdown_requested

    if events_per_second <= 0:
        raise ValueError("events_per_second must be greater than 0")

    machine_ids = build_machine_ids(machine_count)
    batch_interval = 1.0 / events_per_second
    events_written = 0
    corrupt_written = 0
    started_at = time.monotonic()
    last_log_at = started_at

    logger.info(
        "Starting producer: machines=%s rate=%s/s output=%s corrupt_rate=%.2f",
        machine_count,
        events_per_second,
        output_path,
        corrupt_rate,
    )

    while not _shutdown_requested:
        batch_started = time.monotonic()

        if max_runtime_seconds is not None and batch_started - started_at >= max_runtime_seconds:
            break

        for machine_id in machine_ids:
            if _shutdown_requested:
                break

            event, is_corrupt = build_event(machine_id, corrupt_rate=corrupt_rate)
            write_event(output_path, event)
            events_written += 1
            if is_corrupt:
                corrupt_written += 1

        now = time.monotonic()
        if now - last_log_at >= log_interval_seconds:
            logger.info(
                "Events written: %s total (%s corrupt)",
                events_written,
                corrupt_written,
            )
            last_log_at = now

        elapsed = time.monotonic() - batch_started
        sleep_for = batch_interval - elapsed
        if sleep_for > 0 and not _shutdown_requested:
            time.sleep(sleep_for)

    logger.info(
        "Producer stopped. Events written: %s total (%s corrupt)",
        events_written,
        corrupt_written,
    )
    return events_written


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    signal.signal(signal.SIGINT, _handle_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_shutdown)

    try:
        run_producer()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, exiting.")
    except Exception:
        logger.exception("Producer failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
