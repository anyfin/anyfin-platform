SELECT
  a.id,
  p.display_name AS provider,
  a.account_number,
  a.name,
  a.type,
  a.available_credit,
  a.balance,
  a.currency_code,
  a.metadata::text,
  a.updated_at::text
FROM
  accounts a
LEFT JOIN
  providers p
ON
  a.provider_id = p.id
WHERE
  a.user_id = {customer_id};