-- Snowflake UDF para calcular λ₂‑UNSC
CREATE OR REPLACE FUNCTION unsc_lambda2()
RETURNS FLOAT
LANGUAGE SQL
AS $$
  SELECT
    0.4 * (1 - LEAST(veto_rate / 0.30, 1)) +
    0.3 * (regional_representation_score) +
    0.2 * (avg_resolution_time_score) +
    0.1 * (legitimacy_score)
  FROM unsc_metrics
  WHERE date = CURRENT_DATE;
$$;
