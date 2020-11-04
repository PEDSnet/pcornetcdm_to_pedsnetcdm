CREATE SEQUENCE if not exists pcornet_pedsnet.death_seq;

begin;
INSERT INTO pcornet_pedsnet.death (cause_concept_id, cause_source_concept_id, cause_source_value, death_cause_id, death_date, death_datetime, death_impute_concept_id, death_type_concept_id, person_id, domain_source, site)
SELECT 
coalesce(cs.target_concept_id::int, 44814650) as CAUSE_CONCEPT_ID,  -- this field is number, ICD codes don't fit
null as CAUSE_SOURCE_CONCEPT_ID, -- this field is number, ICD codes don't fit
dc.DEATH_CAUSE as CAUSE_SOURCE_VALUE,--put raw ICD10 codes here, it fits the datatype -VARCHAR, and is useful for downstream analytics
nextval('pcornet_pedsnet.death_seq') as death_cause_id,
d.DEATH_DATE as DEATH_DATE,
(d.death_date ||' 00:00:00')::timestamp as DEATH_DATETIME,-- should this be the same value as d.DEATH_DATE
coalesce (impu.source_concept_id::int, 44814650)  as death_impute_concept_id, 
dt.TARGET_CONCEPT_ID as DEATH_TYPE_CONCEPT_ID,
d.patid::int AS PERSON_ID,
'stlouis' as site,
'PCORNET_DEATH' AS DOMAIN_SOURCE
FROM stlouis_pcornet.Death d
LEFT JOIN stlouis_pcornet.DEATH_CAUSE dc on dc.PATID=d.PATID
left join pcornet_maps.pedsnet_pcornet_valueset_map impu on impu.source_concept_class = 'Death date impute' and target_concept = d.death_date_impute
LEFT JOIN CDMH_STAGING.p2o_death_term_xwalk dt on dt.cdm_tbl='DEATH' AND dt.cdm_column_name='DEATH_SOURCE' AND dt.src_code=d.DEATH_SOURCE
LEFT JOIN CDMH_STAGING.p2o_code_xwalk_standard cs on cs.cdm_tbl ='DEATH_CAUSE' and dc.DEATH_CAUSE_CODE = cs.SRC_CODE_TYPE and dc.DEATH_CAUSE = cs.SOURCE_CODE;

commit;
