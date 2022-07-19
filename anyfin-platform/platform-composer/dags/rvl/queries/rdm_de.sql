SELECT
  -- metadata
  l.customer_id,
  l.submission_id,
  schufa_features.error_message AS schufa_error_message,
  crif_parser.error_message AS crif_error_message,
  capacity.error_message AS capacity_error_message,
  -- features
  internal_lookup_features.* EXCEPT(timestamp,
    lookup_id,
    submission_id),
  schufa_features.* EXCEPT(error_message,
    timestamp,
    lookup_id,
    pit),
  crif_parser.score as num_crif_score,
  capacity.* EXCEPT(lookup_id,
    timestamp,
    external_lookup_id,
    submission_id,
    error_message,
    flags,
    inputs,
    outputs),
  -- dpoints
  dfa.dpoint_30_1m,
  dfa.dpoint_30_3m,
  dfa.dpoint_30_6m,
  dfa.dpoint_30_12m,
  dfa.dpoint_60_12m,
  dfa.dpoint_90_6m,
  dfa.dpoint_90_12m,
  -- old, kept for previous analytics
  dfa.dpoint_90_12m_se,
  -- for DE. TODO: revisit definition
  dfa.dpoint_fpd
FROM
  anyfin.assess_staging.de_lookups l
LEFT JOIN
  `anyfin.credit.dpoint_facts_aggr` dfa
ON
  l.submission_id = dfa.submission_id
  AND l.customer_id = dfa.customer_id
WHERE
  schufa_features.lookup_id IS NOT NULL
  -- only submissions with applications that became loans are interesting for modelling
  AND ARRAY_LENGTH(loan_ids) > 0