begin;
insert into SITE_pedsnet.hash_token(
	person_id, 
	token_01, 
	token_02, 
	token_03, 
	token_04, 
	token_05, 
	token_16,
	token_encryption_key)
select
	person.person_id,
	token_01,
	token_02, 
	token_03, 
	token_04, 
	token_05, 
	token_16,
	'SITE_to_pedsnet' as token_encryption_key
from SITE_pcornet.hash_token hash_token
inner join SITE_pedsnet.person person on hash_token.patid=person.person_source_value;
commit; 
