
CREATE SEQUENCE if not exists SITE_pedsnet.cond_occ_seq;

create or replace function is_date(s varchar) returns boolean as $$
begin
          perform s::date;
          return true;
        exception when others then
                  return false;
end;
$$ language plpgsql;

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
        visit_occurrence_id)
SELECT 
    coalesce(
        case
            when cond.condition_type='09' or cond.condition_type='ICD09' then cr_icd9.concept_id_2
            when cond.condition_type='10' or cond.condition_type='ICD10' then cr_icd10.concept_id_2
            when cond.condition_type='SM' then c_snomed.concept_id
            -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.condition ~ '^V[0-9]{2}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            when cond.condition ~ '^E[0-9]{3}.?[0-9]?$' then cr_icd9.concept_id_2
            when cond.condition ~ '^[0-9]{3}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.condition ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then cr_icd10.concept_id_2
            else NULL
        END, 
    44814650)::int as condition_concept_id,
    coalesce(
        case
            when cond.condition_type='09' then c_icd9.concept_id
            when cond.condition_type='10' then c_icd10.concept_id
            when cond.condition_type='SM' then c_snomed.concept_id
            -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.condition ~ '^V[0-9]{2}.?[0-9]{0,2}$' then c_icd9.concept_id
            when cond.condition ~ '^E[0-9]{3}.?[0-9]?$' then c_icd9.concept_id
            when cond.condition ~ '^[0-9]{3}.?[0-9]{0,2}$' then c_icd9.concept_id
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.condition ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then c_icd10.concept_id
            else NULL
        end,
    44814650)::int as condition_source_concept_id,
    left(cond.condition,248) || ' | ' || cond.condition_type as condition_source_value,
    case 
        when is_date(cond.resolve_date::varchar) then cond.resolve_date::date 
    end as condition_end_date,
    case 
        when is_date(cond.resolve_date::varchar) then cond.resolve_date::timestamp 
    end as condition_end_datetime,
    nextval('SITE_pedsnet.cond_occ_seq') as condition_occurrence_id,
    case 
        when is_date(cond.onset_date::varchar) then cond.onset_date::date
        when is_date(cond.report_date::varchar) then cond.report_date::date
    end as condition_start_date,
    case 
        when is_date(cond.onset_date::varchar) then cond.onset_date::timestamp
        when is_date(cond.report_date::varchar) then cond.report_date::timestamp
    end as condition_start_datetime,
    4230359 AS condition_status_concept_id,
    coalesce(cond.CONDITION_STATUS,cond.raw_condition_status) AS condition_status_source_value,
    2000000089 as condition_type_concept_id,
    person.person_id AS person_id,   
    44814650 as poa_concept_id, 
    vo.provider_id as provider_id,   
    NULL as stop_reason,    
    vo.visit_occurrence_id as visit_occurrence_id    
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
    and c_icd9.vocabulary_id='ICD9CM'
left join vocabulary.concept c_icd10 on cond.condition=c_icd10.concept_code
    and c_icd10.vocabulary_id='ICD10CM'
left join vocabulary.concept c_snomed on cond.condition=c_snomed.concept_code
    and c_snomed.vocabulary_id='SNOMED' and cond.condition_type='SM'
left join vocabulary.concept_relationship cr_icd9
    on c_icd9.concept_id = cr_icd9.concept_id_1
    and cr_icd9.relationship_id='Maps to'
left join vocabulary.concept_relationship cr_icd10
    on c_icd10.concept_id = cr_icd10.concept_id_1
    and cr_icd10.relationship_id='Maps to';
commit;

begin;
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
        visit_occurrence_id)
SELECT 
    coalesce(
        case
            when cond.dx_type='09' or cond.dx_type='ICD09' then cr_icd9.concept_id_2
            when cond.dx_type='10' or cond.dx_type='ICD10' then cr_icd10.concept_id_2
            when cond.dx_type='SM' then c_snomed.concept_id
             -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.dx ~ '^V[0-9]{2}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            when cond.dx ~ '^E[0-9]{3}.?[0-9]?$' then cr_icd9.concept_id_2
            when cond.dx ~ '^[0-9]{3}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.dx ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then cr_icd10.concept_id_2
            else NULL
        end,
    44814650)::int as condition_concept_id,
    nextval('SITE_pedsnet.cond_occ_seq') as condition_occurrence_id,
    coalesce(
        case
            when cond.dx_type='09' or cond.dx_type='ICD09' then c_icd9.concept_id
            when cond.dx_type='10' or cond.dx_type='ICD10' then c_icd10.concept_id
            when cond.dx_type='SM' then c_snomed.concept_id
            -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.dx ~ '^V[0-9]{2}.?[0-9]{0,2}$' then c_icd9.concept_id
            when cond.dx ~ '^E[0-9]{3}.?[0-9]?$' then c_icd9.concept_id
            when cond.dx ~ '^[0-9]{3}.?[0-9]{0,2}$' then c_icd9.concept_id
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.dx ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then c_icd10.concept_id
            else NULL
        end,
    44814650)::int as condition_source_concept_id,
    left(cond.dx,248) || ' | ' || dx_type as condition_source_value,
    case when is_date(cond.dx_date::varchar) then cond.dx_date::date
    when is_date(cond.admit_date::varchar) then cond.admit_date::date
    end as condition_start_date,
    case when is_date(cond.dx_date::varchar) then cond.dx_date::timestamp
    when is_date(cond.admit_date::varchar) then cond.admit_date::timestamp
    end as condition_start_datetime,
    4230359 AS condition_status_concept_id,
    coalesce(cond.dx_source,cond.RAW_DX_SOURCE) AS condition_status_source_value,
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
    else 44814650
    end as condition_type_concept_id,
    person.person_id AS person_id,   
    coalesce(
        case 
            when dx_poa='Y' then 4188539 
            else 4188540 
        end,
        44814650)::int as poa_concept_id, 
    vo.provider_id as provider_id,   
    NULL as stop_reason,    
    vo.visit_occurrence_id as visit_occurrence_id
FROM SITE_pcornet.diagnosis cond
inner join SITE_pedsnet.person person 
    on cond.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
    on cond.encounterid=vo.visit_source_value
left join vocabulary.concept c_icd9 on cond.dx=c_icd9.concept_code
    and c_icd9.vocabulary_id='ICD9CM' 
left join vocabulary.concept c_icd10 on cond.dx=c_icd10.concept_code
    and c_icd10.vocabulary_id='ICD10CM'
left join vocabulary.concept c_snomed on cond.dx=c_snomed.concept_code
    and c_snomed.vocabulary_id='SNOMED' and cond.dx_type='SM'
left join vocabulary.concept_relationship cr_icd9
    on c_icd9.concept_id = cr_icd9.concept_id_1
    and cr_icd9.relationship_id='Maps to'
left join vocabulary.concept_relationship cr_icd10
    on c_icd10.concept_id = cr_icd10.concept_id_1
    and cr_icd10.relationship_id='Maps to';
commit;