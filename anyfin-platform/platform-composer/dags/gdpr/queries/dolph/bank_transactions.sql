SELECT
  account_id,
  user_id,
  date::text,
  description,
  amount,
  currency_code,
  type,
  category_metadata::text,
  metadata::text,
  updated_at::text
FROM
  transactions
WHERE
  user_id = {customer_id}
ORDER BY
  account_id,
  date;