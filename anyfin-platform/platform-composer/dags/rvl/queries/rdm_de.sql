SELECT
  l.submission_id,
  l.customer_id,
  internal_lookup_features.* EXCEPT(timestamp,
    lookup_id,
    submission_id),
  schufa_features.* EXCEPT(error_message,
    timestamp,
    lookup_id),
  capacity.* EXCEPT(lookup_id,
    timestamp,
    external_lookup_id,
    submission_id),
    dfa.dpoint_30_1m,
    dfa.dpoint_30_3m,
    dfa.dpoint_30_6m,
    dfa.dpoint_30_12m,
    dfa.dpoint_60_12m,
    dfa.dpoint_90_6m,
    dfa.dpoint_90_12m,
    dfa.dpoint_90_12m_se,
    dfa.dpoint_fpd
FROM
  anyfin.assess_staging.de_lookups l
LEFT JOIN `anyfin.credit.dpoint_facts_aggr` dfa ON l.submission_id = dfa.submission_id AND l.customer_id = dfa.customer_id
WHERE ARRAY_LENGTH(loan_ids) > 0
AND internal_lookup_id is not null
AND schufa_features.lookup_id is not null