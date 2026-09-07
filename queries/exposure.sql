SELECT
    normalized_rate,
    rate_unit,
    gross_exposure,
    residual_exposure_after_verification,
    monetary_interpretation
FROM decision_impact_scenarios
ORDER BY CAST(normalized_rate AS DECIMAL(18, 6));
