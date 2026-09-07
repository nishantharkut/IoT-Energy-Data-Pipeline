WITH expanded_quality AS (
    SELECT
        e.source_event_id,
        e.meter_id,
        e.event_time_utc,
        explode_outer(e.quality_flags) AS quality_flag
    FROM gold_source_events AS e
)
SELECT
    source_event_id,
    meter_id,
    event_time_utc,
    quality_flag
FROM expanded_quality
WHERE quality_flag IS NOT NULL
ORDER BY quality_flag, meter_id, event_time_utc, source_event_id;
