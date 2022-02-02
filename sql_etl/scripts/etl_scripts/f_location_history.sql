create sequence if not exists SITE_pedsnet.loc_hist_seq;

begin;

insert into SITE_pedsnet.location_history(
	location_history_id,
	domain_id, 
	end_date, 
	end_datetime, 
	entity_id, 
	location_id, 
	location_preferred_concept_id, 
	relationship_type_concept_id, 
	start_date, 
	start_datetime,
	site
)
select
	nextval('SITE_pedsnet.loc_hist_seq')::bigint as location_history_id,
	'Person' as domain_id,
	address_period_end::date as end_date,
	address_period_end::timestamp as end_datetime,
	person.person_id as entity_id,
	loc.location_id as location_id,
	44814653 as location_preferred_concept_id,
	0 as relationship_type_concept_id,
	address_period_start::date as start_date,
	address_period_start::timestamp as start_datetime,
	'SITE' as site
from SITE_pcornet.lds_address_history lds
inner join SITE_pedsnet.person person on lds.patid=person.person_source_value
inner join SITE_pedsnet.location loc on address_zip5=loc.zip
;

commit;