begin;

Insert into pcornet_pedsnet.person (PERSON_ID, birth_date, birth_datetime, care_site_id, day_of_birth, 
ethnicity_concept_id, ethnicity_source_concept_id, ethnicity_source_value, gender_concept_id, 
gender_source_concept_id, gender_source_value, language_concept_id, language_source_concept_id, language_source_value, 
location_id, month_of_birth, person_source_value, pn_gestational_age, provider_id, 
race_concept_id, race_source_concept_id, race_source_value, year_of_birth, domain_source, site)
SELECT 
distinct on (demo.patid) demo.patid::int AS PERSON_ID, 
demo.birth_date as birth_date,
(demo.birth_date || ' 00:00:00')::timestamp as BIRTH_DATETIME,
44814650 AS CARE_SITE_ID,
extract(day from demo.birth_date) AS DAY_OF_BIRTH,
ex.TARGET_CONCEPT_ID AS ethnicity_concept_id,
0 AS ethnicity_source_concept_id,
demo.raw_hispanic AS ethnicity_source_value, 
gx.TARGET_CONCEPT_ID AS gender_concept_id,
0 as gender_source_concept_id,
demo.raw_gender_identity AS gender_source_value,
coalesce(lang.source_concept_id::int, 44814650) as language_concept_id,
0 as language_source_concept_id,
raw_pat_pref_language_spoken as language_source_value,
null AS LOCATIONID,
EXTRACT(MONTH FROM BIRTH_DATE) AS MONTH_OF_BIRTH,
demo.PATID AS person_source_value, 
null as pn_gestational_age, 
NULL AS PROVIDER_ID, 
CASE WHEN demo.RACE != '06' OR (demo.RACE='06' AND demo.raw_race is null) then rx.TARGET_CONCEPT_ID
                ELSE 44814650
                END AS race_concept_id,
0 AS race_source_concept_id, 				
demo.raw_race AS race_source_value,
EXTRACT(YEAR FROM BIRTH_DATE) AS YEAR_OF_BIRTH, 
'PCORNET_DEMOGRAPHIC' AS DOMAIN_SOURCE,
'SITE' as site
FROM SITE_pcornet.DEMOGRAPHIC demo
left join pcornet_maps.pedsnet_pcornet_valueset_map lang on source_concept_class = 'Language' 
           and source_concept_id is not null 
		   and lang.target_concept = demo.pat_pref_language_spoken
LEFT JOIN CDMH_STAGING.Gender_Xwalk gx on gx.CDM_TBL='DEMOGRAPHIC'AND Gx.Src_Gender=demo.Sex 
LEFT JOIN CDMH_STAGING.ETHNICITY_XWALK ex on ex.CDM_TBL='DEMOGRAPHIC' AND demo.HISPANIC=Ex.Src_Ethnicity 
LEFT JOIN CDMH_STAGING.RACE_XWALK rx on rx.CDM_TBL='DEMOGRAPHIC' AND demo.RACE=rx.Src_Race ;

-- commit;