begin;
INSERT INTO SITE_pedsnet.procedure_occurrence (
      procedure_occurrence_id, 
      modifier_concept_id,
      modifier_source_value, 
      person_id, 
      procedure_concept_id, 
      procedure_date, 
      procedure_end_date,
      procedure_datetime, 
      procedure_end_datetime, 
      procedure_source_concept_id, 
      procedure_source_value, 
      procedure_type_concept_id, 
      provider_id, 
      quantity, 
      visit_occurrence_id, 
      site)
SELECT 
      distinct 
      row_number() over (order by proc.proceduresid)::bigint AS PROCEDURE_OCCURRENCE_ID,
      0 as modifier_concept_id,
      null as modifier_source_value,
      person.person_id,
      coalesce(case
            when c_hcpcs.concept_id is not null then c_hcpcs.concept_id
            when proc.px_type='CH' then c_cpt.concept_id
            when proc.px_type='10' then c_icd9.concept_id
            when proc.px_type='09' then c_icd10.concept_id
            else 0 
      end, 0) as procedure_concept_id,
      coalesce(proc.px_date,proc.admit_date) as procedure_date,
      null::date as procedure_end_date,
      proc.px_date::timestamp as procedure_datetime,
      null::timestamp as procedure_end_datetime,
      case
            when c_hcpcs.concept_id is not null then c_hcpcs.concept_id
            when proc.px_type='CH' then c_cpt.concept_id
            when proc.px_type='10' then c_icd9.concept_id
            when proc.px_type='09' then c_icd10.concept_id
      else 0 
      end as procedure_source_concept_id,
      proc.proceduresid as procedure_source_value,
      case 
            when proc.px_source = 'OD' then 38000275
            when proc.px_source ='BI' then 44786631
            else 44814650
      end AS procedure_type_concept_id,   
      vo.provider_id,
      null::bigint as quantity,
      vo.visit_occurrence_id,
      'SITE' as site
FROM SITE_pcornet.procedures proc
inner join SITE_pedsnet.person person 
      on proc.patid = person.person_source_value
inner join SITE_pedsnet.visit_occurrence vo 
      on proc.encounterid = vo.visit_source_value
left join vocabulary.concept c_hcpcs
      on proc.px=c_hcpcs.concept_code and proc.px_type='CH' and c_hcpcs.vocabulary_id='HCPCS' and proc.px ~ '[A-Z]'
left join vocabulary.concept c_cpt
      on proc.px=c_cpt.concept_code and proc.px_type='CH' and c_cpt.vocabulary_id='CPT4'
left join vocabulary.concept c_icd10
      on proc.px=c_icd10.concept_code and proc.px_type='10' and c_cpt.vocabulary_id='ICD10CM'
 left join vocabulary.concept c_icd9
      on proc.px=c_icd9.concept_code and proc.px_type='09' and c_cpt.vocabulary_id='ICD9CM';

commit;													   