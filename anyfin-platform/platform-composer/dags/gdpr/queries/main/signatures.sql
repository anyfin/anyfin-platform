select 
	created_at::text, 
	content->'user'->>'name' as signee_name, 
	content->'user'->>'personalNumber' as signee_pno, 
	content->'device'->>'ipAddress' as signee_ip,
	CONVERT_FROM(DECODE(
		(xpath(
	      '/a:Signature/a:Object/b:bankIdSignedData/b:usrVisibleData/text()',
	      CONVERT_FROM(DECODE(content->>'signature', 'BASE64'), 'UTF-8')::xml,
	      '{{
	      	{{a,http://www.w3.org/2000/09/xmldsig#}},
	      	{{b,http://www.bankid.com/signature/v1.0.0/types}}
	      }}'
	    ))[1]::text,
	'BASE64'), 'UTF-8') as signed_text,
	platform, user_ip, user_agent
from signatures where customer_id = {customer_id};