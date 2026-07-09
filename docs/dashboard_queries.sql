-- =============================================================================
-- Smart Factory — Automated Pipeline Dashboard (6 artifacts)
-- =============================================================================
--
-- Views are created by notebooks/00_setup.py (run once in Databricks).
-- Use these queries as datasets in AI/BI Dashboard.
-- See docs/DASHBOARD_SETUP.md for tile configuration.
--
-- Views available after setup:
--   workspace.default.bronze_events
--   workspace.default.silver_events
--   workspace.default.gold_machine_metrics
--   workspace.default.pipeline_runs
--   workspace.default.pipeline_health

-- =============================================================================
-- ARTIFACT 1 — Pipeline Flow (TABLE)
-- Shows data at each medallion layer + where it lives
-- Visualization: Table
-- =============================================================================
SELECT sort_order, stage, location, row_count, last_updated, what_happens
FROM (
    SELECT
        1 AS sort_order,
        '① Landing' AS stage,
        '/Volumes/.../smart_factory/landing/' AS location,
        (SELECT bronze_rows FROM workspace.default.pipeline_health) AS row_count,
        CAST((SELECT last_bronze_timestamp FROM workspace.default.pipeline_health) AS STRING) AS last_updated,
        'Producer writes JSON files automatically' AS what_happens

    UNION ALL

    SELECT
        2,
        '② Bronze (Raw)',
        '/Volumes/.../tables/bronze_events',
        (SELECT bronze_rows FROM workspace.default.pipeline_health),
        CAST((SELECT last_bronze_timestamp FROM workspace.default.pipeline_health) AS STRING),
        'All events ingested unchanged — corrupt rows kept'

    UNION ALL

    SELECT
        3,
        '③ Silver (Clean)',
        '/Volumes/.../tables/silver_events',
        (SELECT silver_rows FROM workspace.default.pipeline_health),
        CAST((SELECT last_silver_event_time FROM workspace.default.pipeline_health) AS STRING),
        'Validated, typed, filtered — bad rows removed'

    UNION ALL

    SELECT
        4,
        '④ Gold (Metrics)',
        '/Volumes/.../tables/gold_machine_metrics',
        (SELECT gold_window_rows FROM workspace.default.pipeline_health),
        CAST((SELECT last_gold_window_end FROM workspace.default.pipeline_health) AS STRING),
        '1-minute windowed KPIs per machine'

    UNION ALL

    SELECT
        5,
        '✗ Rejected',
        'Filtered by Silver',
        (SELECT rejected_rows FROM workspace.default.pipeline_health),
        CAST((SELECT last_silver_event_time FROM workspace.default.pipeline_health) AS STRING),
        CONCAT('Quality rate: ', CAST((SELECT quality_rate_pct FROM workspace.default.pipeline_health) AS STRING), '%')
)
ORDER BY sort_order;


-- =============================================================================
-- ARTIFACT 2 — Data Quality Funnel (BAR CHART)
-- Visualization: Bar — X: stage, Y: event_count
-- =============================================================================
SELECT stage, event_count, sort_order
FROM (
    SELECT 'Bronze (Raw)' AS stage, bronze_rows AS event_count, 1 AS sort_order
    FROM workspace.default.pipeline_health
    UNION ALL
    SELECT 'Silver (Valid)', silver_rows, 2
    FROM workspace.default.pipeline_health
    UNION ALL
    SELECT 'Rejected', rejected_rows, 3
    FROM workspace.default.pipeline_health
)
ORDER BY sort_order;


-- =============================================================================
-- ARTIFACT 3 — Ingestion Activity (LINE CHART)
-- Events per minute — proves data is flowing through Silver
-- Visualization: Line — X: minute, Y: event_count
-- =============================================================================
SELECT
    DATE_TRUNC('minute', event_time) AS minute,
    COUNT(*) AS event_count,
    COUNT(DISTINCT machine_id) AS active_machines
FROM workspace.default.silver_events
GROUP BY DATE_TRUNC('minute', event_time)
ORDER BY minute;


-- =============================================================================
-- ARTIFACT 4 — Temperature by Machine (LINE CHART)
-- Visualization: Line — X: window_start, Series: machine_id, Y: avg_temperature
-- =============================================================================
SELECT
    window_start,
    machine_id,
    ROUND(avg_temperature, 1) AS avg_temperature,
    ROUND(max_temperature, 1) AS max_temperature
FROM workspace.default.gold_machine_metrics
ORDER BY window_start, machine_id;


-- =============================================================================
-- ARTIFACT 5 — Machine Health Alerts (TABLE)
-- Visualization: Table
-- =============================================================================
SELECT
    machine_id,
    window_start,
    window_end,
    ROUND(max_temperature, 1) AS max_temperature,
    error_count,
    is_overheating,
    CASE
        WHEN is_overheating AND error_count > 0 THEN 'OVERHEATING + ERROR'
        WHEN is_overheating THEN 'OVERHEATING'
        WHEN error_count > 0 THEN 'ERROR'
        ELSE 'OK'
    END AS alert_type
FROM workspace.default.gold_machine_metrics
WHERE is_overheating = true OR error_count > 0
ORDER BY window_start DESC, machine_id;


-- =============================================================================
-- ARTIFACT 6 — Pipeline Run History (LINE CHART)
-- Row counts growing over time as automated job runs
-- Visualization: Line — X: run_timestamp, Y: row counts (multiple series)
-- =============================================================================
SELECT
    run_timestamp,
    bronze_rows_total AS bronze,
    silver_rows_total AS silver,
    gold_windows_total AS gold,
    landing_files_added,
    quality_rate_pct,
    duration_seconds
FROM workspace.default.pipeline_runs
WHERE status = 'success'
ORDER BY run_timestamp;


-- =============================================================================
-- BONUS — Vibration Trend (LINE CHART) — optional 7th tile
-- =============================================================================
SELECT
    window_start,
    machine_id,
    ROUND(avg_vibration, 2) AS avg_vibration,
    event_count
FROM workspace.default.gold_machine_metrics
ORDER BY window_start, machine_id;
