
CREATE SEQUENCE if not exists SITE_pedsnet.cond_occ_seq;

begin;
-- problem_list
INSERT INTO SITE_pedsnet.condition_occurrence(
        condition_concept_id,
        condition_source_concept_id,
        condition_source_value,
        condition_end_date,
        condition_end_datetime,
        condition_occurrence_id,
        condition_start_date,
        condition_start_datetime, 
        condition_status_concept_id,
        condition_status_source_value,
        condition_type_concept_id,
        person_id,
        poa_concept_id, 
        provider_id,
        stop_reason,
        visit_occurrence_id,
        site)
SELECT 
    coalesce(case
        when cond.condition_type='09' then cr_icd9.concept_id_2
        when cond.condition_type='10' then cr_icd10.concept_id_2
        when cond.condition_type='SM' then c_snomed.concept_id
    else
     0
    end,44814650)::int as condition_concept_id,
    coalesce(case
        when cond.condition_type='09' then c_icd9.concept_id
        when cond.condition_type='10' then c_icd10.concept_id
        when cond.condition_type='SM' then c_snomed.concept_id
    else
        0
    end,44814650)::int as condition_source_concept_id,
    cond.condition as condition_source_value,
    cond.resolve_date as condition_end_date, 
    cond.resolve_date::timestamp as condition_end_datetime,
    nextval('SITE_pedsnet.cond_occ_seq')::bigint as condition_occurrence_id,
    coalesce(cond.onset_date,cond.report_date) as condition_start_date, 
    coalesce(cond.onset_date,cond.report_date)::timestamp as condition_start_datetime,
    4230359 AS condition_status_concept_id,
    cond.raw_condition_status AS condition_status_source_value,
    2000000089 as condition_type_concept_id,
    person.person_id AS person_id,   
    null as poa_concept_id, 
    vo.provider_id as provider_id,   
    NULL as stop_reason,    
    vo.visit_occurrence_id as visit_occurrence_id, 
    'SITE' as site    
FROM (
    select *
    from SITE_pcornet.condition
    where encounterid is not null
) as cond
inner join SITE_pedsnet.person person 
    on cond.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
    on cond.encounterid=vo.visit_source_value
left join vocabulary.concept c_icd9 on cond.condition=c_icd9.concept_code
    and c_icd9.vocabulary_id='ICD9CM' and cond.condition_type='09'
left join vocabulary.concept c_icd10 on cond.condition=c_icd10.concept_code
    and c_icd10.vocabulary_id='ICD10CM' and cond.condition_type='10'
left join vocabulary.concept c_snomed on cond.condition=c_snomed.concept_code
    and c_snomed.vocabulary_id='SNOMED' and cond.condition_type='SM'
left join vocabulary.concept_relationship cr_icd9
    on c_icd9.concept_id = cr_icd9.concept_id_1
    and cr_icd9.relationship_id='Maps to'
left join vocabulary.concept_relationship cr_icd10
    on c_icd10.concept_id = cr_icd10.concept_id_1
    and cr_icd10.relationship_id='Maps to'
;

commit;

-- visit diagnoses

INSERT INTO SITE_pedsnet.condition_occurrence(
        condition_concept_id,
        condition_occurrence_id,
        condition_source_concept_id,
        condition_source_value,
        condition_start_date,
        condition_start_datetime, 
        condition_status_concept_id,
        condition_status_source_value,
        condition_type_concept_id,
        person_id,
        poa_concept_id, 
        provider_id,
        stop_reason,
        visit_occurrence_id,
        site)
SELECT 
    coalesce(case
        when cond.dx_type='09' then cr_icd9.concept_id_2
        when cond.dx_type='10' then cr_icd10.concept_id_2
        when cond.dx_type='SM' then c_snomed.concept_id
    else
     0
    end,44814650)::int as condition_concept_id,
    nextval('SITE_pedsnet.cond_occ_seq')::bigint as condition_occurrence_id,
    coalesce(case
        when cond.dx_type='09' then c_icd9.concept_id
        when cond.dx_type='10' then c_icd10.concept_id
        when cond.dx_type='SM' then c_snomed.concept_id
    else
        0
    end,44814650)::int as condition_source_concept_id,
    cond.dx as condition_source_value,
    coalesce(cond.dx_date,cond.admit_date) as condition_start_date, 
    coalesce(cond.dx_date,cond.admit_date)::timestamp as condition_start_datetime,
    4230359 AS condition_status_concept_id,
    cond.dx_source AS condition_status_source_value,
    case 
        when enc_type='ED' and dx_origin='BI' and pdx='P'then 2000001282
        when enc_type='ED' and dx_origin='OD' and pdx='P'then 2000001280
        when enc_type='ED' and dx_origin='CL' and pdx='P'then 2000001281
        when enc_type='ED' and dx_origin='BI' and pdx='S'then 2000001284
        when enc_type='ED' and dx_origin='OD' and pdx='S'then 2000001283
        when enc_type='ED' and dx_origin='CL' and pdx='S'then 2000001285
        when enc_type in ('AV','OA','TH') and dx_origin='BI' and pdx='P'then 2000000096
        when enc_type in ('AV','OA','TH') and dx_origin='OD' and pdx='P'then 2000000095
        when enc_type in ('AV','OA','TH') and dx_origin='CL' and pdx='P'then 2000000097
        when enc_type in ('AV','OA','TH') and dx_origin='BI' and pdx='S'then 2000000102
        when enc_type in ('AV','OA','TH') and dx_origin='OD' and pdx='S'then 2000000101
        when enc_type in ('AV','OA','TH') and dx_origin='CL' and pdx='S'then 2000000103
        when enc_type in ('IP','OS','IS','EI') and dx_origin='BI' and pdx='P'then 2000000093
        when enc_type in ('IP','OS','IS','EI') and dx_origin='OD' and pdx='P'then 2000000092
        when enc_type in ('IP','OS','IS','EI') and dx_origin='CL' and pdx='P'then 2000000094
        when enc_type in ('IP','OS','IS','EI') and dx_origin='BI' and pdx='S'then 2000000099
        when enc_type in ('IP','OS','IS','EI') and dx_origin='OD' and pdx='S'then 2000000098
        when enc_type in ('IP','OS','IS','EI') and dx_origin='CL' and pdx='S'then 2000000100
    else 
        44814650
    end  as condition_type_concept_id,
    person.person_id AS person_id,   
    coalesce(case when dx_poa='Y' then 4188539 else 4188540 end,44814650)::int as poa_concept_id, 
    vo.provider_id as provider_id,   
    NULL as stop_reason,    
    vo.visit_occurrence_id as visit_occurrence_id, 
    'SITE' as site
FROM SITE_pcornet.diagnosis cond
inner join SITE_pedsnet.person person 
    on cond.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
    on cond.encounterid=vo.visit_source_value
left join vocabulary.concept c_icd9 on cond.dx=c_icd9.concept_code
    and c_icd9.vocabulary_id='ICD9CM' and cond.dx_type='09'
left join vocabulary.concept c_icd10 on cond.dx=c_icd10.concept_code
    and c_icd10.vocabulary_id='ICD10CM' and cond.dx_type='10'
left join vocabulary.concept c_snomed on cond.dx=c_snomed.concept_code
    and c_snomed.vocabulary_id='SNOMED' and cond.dx_type='SM'
left join vocabulary.concept_relationship cr_icd9
    on c_icd9.concept_id = cr_icd9.concept_id_1
    and cr_icd9.relationship_id='Maps to'
left join vocabulary.concept_relationship cr_icd10
    on c_icd10.concept_id = cr_icd10.concept_id_1
    and cr_icd10.relationship_id='Maps to'
;
commit;


