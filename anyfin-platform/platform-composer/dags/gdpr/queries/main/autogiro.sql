SELECT
  created_at::text,
  personal_identifier,
  account_number,
  status
FROM
  autogiro_authorizations
WHERE
  customer_id = {customer_id};