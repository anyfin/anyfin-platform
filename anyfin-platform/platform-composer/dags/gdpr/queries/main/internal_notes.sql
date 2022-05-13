SELECT
  id,
  created_at::text,
  created_by,
  content,
  customer_id,
  application_id, 
  files::text,
  type
FROM
  notes
WHERE
  customer_id = {customer_id};