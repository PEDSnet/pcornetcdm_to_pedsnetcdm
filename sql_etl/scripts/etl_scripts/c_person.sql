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
    race_source_value)
SELECT distinct 
  demo.patid AS person_id, 
  demo.birth_date::date as birth_date,
  (demo.birth_date || ' 00:00:00')::timestamp as birth_datetime,
  9999999 AS care_site_id,
  extract(day from demo.birth_date::date) AS day_of_birth,
  extract(month from birth_date::date) AS month_of_birth,
  extract(year from birth_date::date) AS year_of_birth,
  case 
      when ethnicity_map.target_concept = 'OT' then 44814649
	    when ethnicity_map.source_concept_id !~ '^[0-9]+$' then 44814650
      else coalesce(ethnicity_map.source_concept_id::int, 44814650) 
  end AS ethnicity_concept_id,
  44814650 AS ethnicity_source_concept_id,
  coalesce(ethnicity_map.concept_description,'') || ' | ' || coalesce(demo.hispanic,demo.raw_hispanic) AS ethnicity_source_value,
  case 
      when gender_map.target_concept = 'OT' then 44814649
      when gender_map.source_concept_id !~ '^[0-9]+$' then 44814650
      else coalesce(gender_map.source_concept_id::int,44814650) 
  end AS gender_concept_id,
  44814650 as gender_source_concept_id,
  coalesce(gender_map.concept_description,'') || ' | ' || coalesce(demo.sex,demo.raw_sex) AS gender_source_value,
  case
      when lang.source_concept_id !~ '^[0-9]+$' then 44814650
	    else coalesce(lang.source_concept_id::int, 44814650)
  end	as language_concept_id,
  44814650 as language_source_concept_id,
  coalesce(lang.concept_description,'') || ' | ' || coalesce(PAT_PREF_LANGUAGE_SPOKEN,raw_pat_pref_language_spoken) as language_source_value,
  9999999 AS location_id,
  demo.patid AS person_source_value, 
  null::numeric as pn_gestational_age, 
  ppp.provider_id AS provider_id,
  case
      when race_map.source_concept_id !~ '^[0-9]+$' then 44814650
	    else coalesce(race_map.source_concept_id::int,44814650)
      end as race_concept_id,
  44814650 AS race_source_concept_id, 				
  coalesce(race_map.concept_description,'') || ' | ' || coalesce(demo.race,demo.raw_race) AS race_source_value
FROM 
  SITE_pcornet.DEMOGRAPHIC demo
left join 
  SITE_pedsnet.person_primary_provider ppp
  on ppp.patid = demo.patid
left join 
  pcornet_maps.pcornet_pedsnet_valueset_map lang 
  on source_concept_class = 'Language' 
  and source_concept_id is not null 
	and lang.target_concept = demo.pat_pref_language_spoken
left join 
  pcornet_maps.pcornet_pedsnet_valueset_map gender_map 
  on demo.sex=gender_map.target_concept
  and gender_map.source_concept_class='Gender'
left join 
  pcornet_maps.pcornet_pedsnet_valueset_map ethnicity_map 
  on demo.hispanic = ethnicity_map.target_concept
  and ethnicity_map.source_concept_class='Hispanic'
left join 
  pcornet_maps.pcornet_pedsnet_valueset_map race_map
  on demo.race=race_map.target_concept
  and race_map.source_concept_class = 'Race'
ON CONFLICT (person_id) DO NOTHING;
commit;

-- begin;
-- with x_walk as (
--     select 
--         geo.addressid as patid,
--         loc.location_id
--     from 
--         SITE_pcornet.PRIVATE_ADDRESS_GEOCODE geo 
--     inner join 
--         SITE_pedsnet.location loc
--         on loc.census_block_group = geo.GEOCODE_BLOCK
-- )
--  Update SITE_pedsnet.person p
--  set location_id = x_walk.location_id
--  from x_walk
--  where p.person_id::varchar = x_walk.patid;
--  commit;SITE_pedsnet.person
