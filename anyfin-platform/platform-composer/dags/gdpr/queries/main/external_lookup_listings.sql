SELECT
  created_at::text,
  main_policy->'data'->'ReverseLookup'->>'_id' AS spar_id,
  (main_policy->'data'->'ReverseLookup'->>'_ts')::timestamp::text AS spar_ts,
  main_policy->'data'->'UCLookup'->>'_id' AS uc_id,
  (main_policy->'data'->'UCLookup'->>'_ts')::timestamp::text AS uc_ts
FROM
  assessments
WHERE
  customer_id = {customer_id};