SELECT
  a.created_at::text,
  a.source,
  es.customer_name,
  es.customer_street,
  es.customer_postal,
  lenders.display_name AS lender,
  es.loan_balance,
  a.currency_code,
  es.payment_account,
  es.payment_reference,
  a.status,
  a.reject_reason,
  es.status,
  ass.status
FROM
  applications a
JOIN
  external_statements es
ON
  a.external_statement_id = es.id
LEFT JOIN
  lenders
ON
  es.lender_id = lenders.id
LEFT JOIN
  assessments ass
ON
  a.assessment_id = ass.id
WHERE
  a.customer_id = {customer_id};