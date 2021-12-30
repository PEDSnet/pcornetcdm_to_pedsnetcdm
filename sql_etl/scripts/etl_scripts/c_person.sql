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
    NULL AS PROVIDER_ID, 
    coalesce(ethnicity_map.sourcE_concept_id::int,44814650) race_concept_id,
    0 AS race_source_concept_id, 				
    demo.raw_race AS race_source_value,
    'SITE' as site
FROM SITE_pcornet.DEMOGRAPHIC demo
left join pcornet_maps.pedsnet_pcornet_valueset_map lang on source_concept_class = 'Language' 
           and source_concept_id is not null 
		   and lang.target_concept = demo.pat_pref_language_spoken
left join pcornet_maps.pedsnet_pcornet_valueset_map gender_map 
            on demo.sex=gender_maps.target_concept
            and gender_map.source_concept_class='Gender'
left join pcornet_maps.pedsnet_pcornet_valueset_map ethnicity_map 
            on demo.hispanic = ethnicity_map.target_concept
            and ethnicity_map.source_concept_class='Hispanic'
left join pcornet_maps.pedsnet_pcornet_valueset_map race_map
            on demo.race=race_map.target_concept 
            and race_map.source_concept_class = 'Race';
commit;