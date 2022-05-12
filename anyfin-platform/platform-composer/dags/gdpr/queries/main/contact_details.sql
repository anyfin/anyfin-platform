SELECT
  ch.created_at::text,
  ch.type,
  ch.value,
  ch.is_active
FROM
  channels ch
JOIN
  customer_channels cc
ON
  ch.id = cc.channel_id
  AND cc.customer_id = {customer_id};