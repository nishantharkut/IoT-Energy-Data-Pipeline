SELECT
    e.source_event_id,
    e.meter_id,
    e.brick_building_ids,
    e.event_time_utc,
    e.scaled_value,
    e.decimal_scale,
    e.unit
FROM gold_source_events AS e
CROSS JOIN query_parameters AS p
WHERE array_contains(e.brick_building_ids, p.building_id)
ORDER BY e.meter_id, e.event_time_utc, e.source_event_id;
