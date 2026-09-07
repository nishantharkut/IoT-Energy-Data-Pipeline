SELECT
    e.source_event_id,
    e.meter_id,
    e.event_time_utc,
    e.scaled_value,
    e.decimal_scale,
    e.unit,
    e.original_numeric_text,
    e.quality_flags
FROM gold_source_events AS e
CROSS JOIN query_parameters AS p
WHERE e.meter_id = p.meter_id
ORDER BY e.event_time_utc, e.source_event_id;
