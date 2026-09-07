SELECT
    e.source_event_id,
    e.meter_id,
    e.event_time_utc,
    e.scaled_value,
    e.decimal_scale,
    e.unit
FROM gold_source_events AS e
CROSS JOIN query_parameters AS p
WHERE e.event_time_utc >= p.window_start_utc
  AND e.event_time_utc < p.window_end_utc
ORDER BY e.event_time_utc, e.meter_id, e.source_event_id;
