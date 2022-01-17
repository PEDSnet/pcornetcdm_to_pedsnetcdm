create sequence SITE_pedsnet.loc_hist_seq;

begin;

insert into location_history(
	location_history_id,
	domain_id, 
	end_date, 
	end_datetime, 
	entity_id, 
	location_id, 
	location_preferred_concept_id, 
	relationship_type_concept_id, 
	start_date, 
	start_datetime
)
select
	nextval('SITE_pedsnet.loc_hist_seq')::bigint as location_history_id,
	'Person',
	address_period_end::date,
	address_period_end::timestamp,
	person.person_id as entity_id,
	loc.location_id,
	44814653,
	0,
	address_period_start::date,
	address_period_start::timestamp
from SITE_pcornet.lds_address_history lds
inner join SITE_pedsnet.person person on lds.patid=person.person_source_value
left join site_pedsnet.location loc on address_zip5=loc.zip
;

commit;