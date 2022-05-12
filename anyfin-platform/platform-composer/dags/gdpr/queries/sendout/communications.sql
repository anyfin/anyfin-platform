SELECT
  scheduled_at::text,
  status,
  type,
  recipient,
  content::text
FROM
  sendouts
WHERE
  customer_id = {customer_id}
ORDER BY
  scheduled_at;