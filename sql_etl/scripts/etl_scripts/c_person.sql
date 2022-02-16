begin;
insert into SITE_pedsnet.person (
    person_id,
    birth_date, 
    birth_datetime, 
    care_site_id, 
    day_of_birth,
    month_of_birth,
    year_of_birth,  
    ethnicity_concept_id, 
    ethnicity_source_concept_id, 
    ethnicity_source_value, 
    gender_concept_id, 
    gender_source_concept_id, 
    gender_source_value, 
    language_concept_id, 
    language_source_concept_id, 
    language_source_value, 
    location_id, 
    person_source_value, 
    pn_gestational_age, 
    provider_id, 
    race_concept_id, 
    race_source_concept_id, 
    race_source_value, 
    site)

--derive a primary provider for each patient
--find provider with most visits from patient and set as primary
-- if > 1 providers have same max number of visits, takes most recent visit 
with provider_history as (
select
	demo.patid, 
	enc.providerid, 
	max((enc.admit_date + enc.admit_time::time)::timestamp) as most_recent_date, 
	count(enc.providerid) as num_visits
from SITE_pcornet.demographic demo
left join SITE_pcornet.encounter enc
on demo.patid = enc.patid
group by 
	demo.patid, 
	enc.providerid
),
get_provider_max as (
select 
	provider_history.patid, 
	provider_history.providerid, 
	provider_history.num_visits,
	provider_history.most_recent_date
from provider_history
inner join (
	select patid, max(num_visits) as max_visit
		from provider_history
		group by patid
	) as most_visits 
	on provider_history.patid = most_visits.patid
	and provider_history.num_visits = most_visits.max_visit
order by patid, most_recent_date desc
),
primary_provider as (
select 
	pcor_prov.patid,
	peds_prov.provider_id
from (	
	select patid, max(providerid) as providerid
	from get_provider_max
	group by patid 
) as pcor_prov
inner join SITE_pedsnet.provider peds_prov
	on pcor_prov.providerid = peds_prov.provider_source_value
)

SELECT 
    distinct on (demo.patid) demo.patid::bigint AS person_id, 
    demo.birth_date::date as birth_date,
    (demo.birth_date || ' 00:00:00')::timestamp as birth_datetime,
    9999999::bigint AS care_site_id,
    extract(day from demo.birth_date) AS day_of_birth,
    extract(month from birth_date) AS month_of_birth,
    extract(year from birth_date) AS year_of_birth,
    coalesce(ethnicity_map.source_concept_id::int, 44814650) AS ethnicity_concept_id,
    0 AS ethnicity_source_concept_id,
    demo.raw_hispanic AS ethnicity_source_value, 
      coalesce(gender_map.source_concept_id::int,44814650) AS gender_concept_id,
    0 as gender_source_concept_id,
    demo.raw_sex AS gender_source_value,
    coalesce(lang.source_concept_id::int, 44814650) as language_concept_id,
    0 as language_source_concept_id,
    raw_pat_pref_language_spoken as language_source_value,
    9999999::bigint AS location_id,
    demo.patid AS person_source_value, 
    null as pn_gestational_age, 
    primary_provider.provider_id AS provider_id, 
    coalesce(ethnicity_map.sourcE_concept_id::int,44814650) race_concept_id,
    0 AS race_source_concept_id, 				
    demo.raw_race AS race_source_value,
    'SITE' as site
FROM SITE_pcornet.DEMOGRAPHIC demo
left join primary_provider on primary_provider.patid = demo.patid
left join pcornet_maps.pedsnet_pcornet_valueset_map lang on source_concept_class = 'Language' 
           and source_concept_id is not null 
		   and lang.target_concept = demo.pat_pref_language_spoken
left join pcornet_maps.pedsnet_pcornet_valueset_map gender_map 
            on demo.sex=gender_map.target_concept
            and gender_map.source_concept_class='Gender'
left join pcornet_maps.pedsnet_pcornet_valueset_map ethnicity_map 
            on demo.hispanic = ethnicity_map.target_concept
            and ethnicity_map.source_concept_class='Hispanic'
left join pcornet_maps.pedsnet_pcornet_valueset_map race_map
            on demo.race=race_map.target_concept 
            and race_map.source_concept_class = 'Race';
commit;