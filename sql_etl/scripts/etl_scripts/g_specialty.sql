CREATE SEQUENCE if not exists SITE_pedsnet.specialty_seq;

begin;

INSERT INTO SITE_pedsnet.specialty(
   specialty_id,
   domain_id,
   entity_id,
   entity_type_concept_id,
   specialty_concept_id,
   specialty_source_value
   )
   
-- provider specialties
select
 	nextval('SITE_pedsnet.specialty_seq') as specialty_id,
	'PROVIDER' as domain_id,
	pro.provider_id as entity_id,
	case 
		when pro.specialty_concept_id is not null then 4114681
		else 4225752 
	end as entity_type_concept_id,
	pro.specialty_concept_id as specialty_concept_id,
	coalesce(pro.specialty_source_value, pro.specialty_concept_id::varchar, ' ') as specialty_source_value
from
	SITE_pedsnet.provider pro
where
	(pro.specialty_concept_id is not null and pro.specialty_concept_id not in (0,44814649,44814650,44814653,38004477))
	or pro.specialty_source_value is not null
	
UNION

--care site specialties
select
  	nextval('SITE_pedsnet.specialty_seq') as specialty_id,
	'CARE_SITE' as domain_id,
	cs.care_site_id as entity_id,
	case 
		when cs.specialty_concept_id is not null then 4114681
		else 4225752 
	end as entity_type_concept_id,	
	cs.specialty_concept_id as specialty_concept_id,
	coalesce(cs.specialty_source_value, cs.specialty_concept_id::varchar, ' ') as specialty_source_value
from
	SITE_pedsnet.care_site cs
where
	(cs.specialty_concept_id is not null and cs.specialty_concept_id not in (0,44814649,44814650,44814653,38004477))
	or cs.specialty_source_value is not null;

commit;
	
