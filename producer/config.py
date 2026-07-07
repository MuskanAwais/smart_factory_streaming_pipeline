"""Configuration for the IoT event producer."""

from pathlib import Path

# Number of simulated machines (IDs: machine_01 .. machine_NN)
MACHINE_COUNT = 10

# Events emitted per machine per second
EVENTS_PER_SECOND = 1.0

# Landing folder for JSON event files
OUTPUT_PATH = Path("data/landing")

# Fraction of events intentionally corrupted for Silver-layer testing
CORRUPT_RATE = 0.05

# How often to log progress (seconds)
LOG_INTERVAL_SECONDS = 5
