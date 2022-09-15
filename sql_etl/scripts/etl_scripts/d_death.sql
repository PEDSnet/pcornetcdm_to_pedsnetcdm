CREATE SEQUENCE if not exists SITE_pedsnet.death_seq;

begin;
INSERT INTO SITE_pedsnet.death (
	cause_concept_id, 
	cause_source_concept_id, 
	cause_source_value, 
	death_cause_id, 
	death_date, 
	death_datetime, 
	death_impute_concept_id, 
	death_type_concept_id, 
	person_id)
SELECT 
coalesce(case
	when dc.death_cause_code='09' then cr_icd9.concept_id_2
	when dc.death_cause_code='10' then cr_icd10.concept_id_2
	when dc.death_cause_code='SM' then c_snomed.concept_id
	when dc.death_cause is not null and (cr_icd9.concept_id_2 is null 
											and cr_icd10.concept_id_2 is null
											and c_snomed.concept_id is null) then 0 end,
	44814650)::int as cause_concept_id,
coalesce(case
	when dc.death_cause_code='09' then c_icd9.concept_id
	when dc.death_cause_code='10' then c_icd10.concept_id
	when dc.death_cause_code='SM' then c_snomed.concept_id
	when dc.death_cause is not null and (c_icd9.concept_id is null 
											and c_icd10.concept_id is null
											and c_snomed.concept_id is null) then 0 end,
	44814650)::int as cause_source_concept_id,
dc.death_cause as cause_source_value,
nextval('SITE_pedsnet.death_seq') as death_cause_id,
case when d.death_date is not null then d.death_date::date end as death_date,
case when d.death_date is not null then (d.death_date::date ||' 00:00:00')::timestamp
end as death_datetime,
coalesce (impu.source_concept_id::int, 44814650)  as death_impute_concept_id, 
coalesce(dt.target_concept_id,0) as death_type_concept_id,
person.person_id as person_id
FROM SITE_pcornet.death d
inner join SITE_pedsnet.person person on d.patid=person.person_source_value
left join SITE_pcornet.death_cause dc on dc.patid=d.patid
left join pcornet_maps.pedsnet_pcornet_valueset_map impu 
	on impu.source_concept_class = 'death date impute' 
	and impu.target_concept = d.death_date_impute
left join cdmh_staging.p2o_death_term_xwalk dt 
	on dt.cdm_tbl='DEATH' 
	and dt.cdm_column_name='DEATH_SOURCE' 
	and dt.src_code=d.death_source
left join vocabulary.concept c_icd9 on dc.death_cause=c_icd9.concept_code
	and c_icd9.vocabulary_id='ICD9CM' and dc.death_cause_code='09'
left join vocabulary.concept c_icd10 on dc.death_cause=c_icd10.concept_code
	and c_icd10.vocabulary_id='ICD10CM' and dc.death_cause_code='10'
left join vocabulary.concept c_snomed on dc.death_cause=c_snomed.concept_code
	and c_snomed.vocabulary_id='SNOMED' and dc.death_cause_code='SM'
left join vocabulary.concept_relationship cr_icd9
	on c_icd9.concept_id = cr_icd9.concept_id_1
	and cr_icd9.relationship_id='Maps to'
left join vocabulary.concept_relationship cr_icd10
	on c_icd10.concept_id = cr_icd10.concept_id_1
	and cr_icd10.relationship_id='Maps to';

commit;
