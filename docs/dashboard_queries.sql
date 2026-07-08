-- Module 7 — Dashboard SQL queries over gold_machine_metrics
--
-- Step 1: Register a VIEW (Unity Catalog volumes cannot use CREATE TABLE ... LOCATION)
-- Run once in Databricks SQL Editor:

CREATE OR REPLACE VIEW workspace.default.gold_machine_metrics AS
SELECT * FROM delta.`/Volumes/workspace/default/smart_factory/tables/gold_machine_metrics`;

-- Verify:
-- SELECT COUNT(*) FROM workspace.default.gold_machine_metrics;

-- =============================================================================
-- AC-7.2 — Average temperature per machine over time (LINE CHART)
-- X-axis: window_start | Series: machine_id | Y-axis: avg_temperature
-- =============================================================================
SELECT
    window_start,
    machine_id,
    avg_temperature
FROM workspace.default.gold_machine_metrics
ORDER BY window_start, machine_id;

-- =============================================================================
-- AC-7.3 — Machines currently in error (TABLE / COUNTER)
-- Latest completed window per machine where error_count > 0
-- =============================================================================
WITH latest_window AS (
    SELECT MAX(window_end) AS max_window_end
    FROM workspace.default.gold_machine_metrics
)
SELECT
    g.machine_id,
    g.window_start,
    g.window_end,
    g.error_count
FROM workspace.default.gold_machine_metrics g
CROSS JOIN latest_window lw
WHERE g.error_count > 0
  AND g.window_end = lw.max_window_end
ORDER BY g.machine_id;

-- =============================================================================
-- AC-7.4 — Overheating alerts (TABLE)
-- =============================================================================
SELECT
    machine_id,
    window_start,
    window_end,
    max_temperature,
    is_overheating
FROM workspace.default.gold_machine_metrics
WHERE is_overheating = true
ORDER BY window_start DESC, machine_id;

-- =============================================================================
-- AC-7.5 — Vibration trend per machine (LINE CHART)
-- X-axis: window_start | Series: machine_id | Y-axis: avg_vibration
-- =============================================================================
SELECT
    window_start,
    machine_id,
    avg_vibration
FROM workspace.default.gold_machine_metrics
ORDER BY window_start, machine_id;
