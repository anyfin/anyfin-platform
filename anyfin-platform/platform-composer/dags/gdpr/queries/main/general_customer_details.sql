SELECT
  full_name,
  ci.value AS personal_identifier,
  address_street,
  address_postcode,
  address_city,
  birthdate,
  gender
FROM
  customers c
LEFT JOIN
  customer_identities ci
ON
  c.id = ci.customer_id
  AND ci.type = 'PERSONAL_ID'
WHERE
  c.id = {customer_id};