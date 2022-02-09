begin;


INSERT INTO SITE_pedsnet.measurement ( 
     measurement_id,
     measurement_concept_id, 
     measurement_date, 
     measurement_datetime,
     measurement_order_date,
     measurement_order_datetime, 
     measurement_result_date, 
     measurement_result_datetime, 
     measurement_source_concept_id, 
     measurement_source_value, 
     measurement_type_concept_id, 
     operator_concept_id, 
     person_id, 
     priority_concept_id, 
     priority_source_value, 
     provider_id, 
     range_high, 
     range_high_operator_concept_id, 
     range_high_source_value,
     range_low, 
     range_low_operator_concept_id, 
     range_low_source_value, 
     specimen_concept_id, 
     specimen_source_value, 
     unit_concept_id,
     unit_source_concept_id, 
     unit_source_value, 
     value_as_concept_id, 
     value_as_number, 
     value_source_value,
     visit_occurrence_id, 
     site)
with 
     lnc as (select concept_id,concept_name,concept_code 
             from vocabulary.concept
             where vocabulary_id='LOINC'
             and standard_concept='S'),
     unit as(select concept_id,concept_name,concept_code 
             from vocabulary.concept
             where vocabulary_id='UCUM'
             and standard_concept='S'),
     qual as(select concept_id,concept_name,concept_code 
             from vocabulary.concept
             where vocabulary_id='SNOMED'
               and concept_class_id='Qualifier Value'
             and standard_concept='S')    
SELECT distinct
     nextval('SITE_pedsnet.measurement_id_seq')::bigint AS measurement_id,
     coalesce(lnc.concept_id,0) AS measurement_concept_id, 
     clin.obsclin_start_date AS measurement_date, 
     (clin.obsclin_start_date || ' '|| clin.obsclin_start_time)::timestamp AS measurement_datetime, 
     null as measurement_order_date, 
     null as measurement_order_datetime, 
     null as measurement_result_date, 
     null as measurement_result_datetime, 
      coalesce(lnc.concept_id,0) AS measurement_source_concept_id,
     coalesce(clin.raw_obsclin_name,lnc.concept_name) AS measurement_source_value, 
     2000000033 as measurement_type_concept_id, 
     case 
          when clin.obsclin_result_modifier ='GE' then 4171755
          when clin.obsclin_result_modifier ='GT' then 4172704
          when clin.obsclin_result_modifier ='LE' then 4171754
          when clin.obsclin_result_modifier ='LT' then 4171756
          else 4172703 -- Equal
          end AS operator_concept_id, 
     person.person_id  AS person_id, 
     0  as priority_concept_id, 
     null as priority_source_value, 
     vo.provider_id as provider_id, 
     null as range_high, 
     null as range_high_operator_concept_id, 
     null as range_high_source_value, 
     null as range_low, 
     null as range_low_operator_concept_id, 
     null as range_low_source_value, 
     null as specimen_concept_id, 
     null as specimen_source_value, 
     coalesce(unit.concept_id,0) AS unit_concept_id,
     coalesce(unit.concept_id,0) AS unit_source_concept_id, 
     coalesce(clin.obsclin_result_unit,clin.raw_obsclin_unit) AS unit_source_value, 
     coalesce(qual.concept_id,0) AS value_as_concept_id, 
     clin.obsclin_result_num AS value_as_number, 
     coalesce(clin.obsclin_result_text,
                 clin.obsclin_result_qual,
                 clin.obsclin_result_num::text) AS value_source_value,
    vo.visit_occurrence_id AS visit_occurrence_id, 
     'SITE' as site
FROM SITE_pcornet.obs_clin clin
inner join SITE_pedsnet.person person on clin.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on clin.encounterid=vo.visit_source_value
left join lnc on clin.obsclin_code=lnc.concept_code
left join unit on clin.obsclin_result_unit=unit.concept_code
left join qual on clin.obsclin_result_qual=qual.concept_code
where obsclin_type='LC';

commit;

INSERT INTO SITE_pedsnet.measurement ( 
     measurement_id,
     measurement_concept_id, 
     measurement_date, 
     measurement_datetime,
     measurement_order_date,
     measurement_order_datetime, 
     measurement_result_date, 
     measurement_result_datetime, 
     measurement_source_concept_id, 
     measurement_source_value, 
     measurement_type_concept_id, 
     operator_concept_id, 
     person_id, 
     priority_concept_id, 
     priority_source_value, 
     provider_id, 
     range_high, 
     range_high_operator_concept_id, 
     range_high_source_value,
     range_low, 
     range_low_operator_concept_id, 
     range_low_source_value, 
     specimen_concept_id, 
     specimen_source_value, 
     unit_concept_id,
     unit_source_concept_id, 
     unit_source_value, 
     value_as_concept_id, 
     value_as_number, 
     value_source_value,
     visit_occurrence_id, 
     site)
with 
     sm as (select concept_id,concept_name,concept_code 
             from vocabulary.concept
             where vocabulary_id='SNOMED'
             and standard_concept='S'),
     unit as(select concept_id,concept_name,concept_code 
             from vocabulary.concept
             where vocabulary_id='UCUM'
             and standard_concept='S'),
     qual as(select concept_id,concept_name,concept_code 
             from vocabulary.concept
             where vocabulary_id='SNOMED'
               and concept_class_id='Qualifier Value'
             and standard_concept='S')    
SELECT distinct
     nextval('SITE_pedsnet.measurement_id_seq')::bigint AS measurement_id,
     coalesce(sm.concept_id,0) AS measurement_concept_id, 
     clin.obsclin_start_date AS measurement_date, 
     (clin.obsclin_start_date || ' '|| clin.obsclin_start_time)::timestamp AS measurement_datetime, 
     null as measurement_order_date, 
     null as measurement_order_datetime, 
     null as measurement_result_date, 
     null as measurement_result_datetime, 
      coalesce(sm.concept_id,0) AS measurement_source_concept_id,
     coalesce(clin.raw_obsclin_name,lnc.concept_name) AS measurement_source_value, 
     2000000033 as measurement_type_concept_id, 
     case 
          when clin.obsclin_result_modifier ='GE' then 4171755
          when clin.obsclin_result_modifier ='GT' then 4172704
          when clin.obsclin_result_modifier ='LE' then 4171754
          when clin.obsclin_result_modifier ='LT' then 4171756
          else 4172703 -- Equal
          end AS operator_concept_id, 
     person.person_id  AS person_id, 
     0  as priority_concept_id, 
     null as priority_source_value, 
     vo.provider_id as provider_id, 
     null as range_high, 
     null as range_high_operator_concept_id, 
     null as range_high_source_value, 
     null as range_low, 
     null as range_low_operator_concept_id, 
     null as range_low_source_value, 
     null as specimen_concept_id, 
     null as specimen_source_value, 
     coalesce(unit.concept_id,0) AS unit_concept_id,
     coalesce(unit.concept_id,0) AS unit_source_concept_id, 
     coalesce(clin.obsclin_result_unit,clin.raw_obsclin_unit) AS unit_source_value, 
     coalesce(qual.concept_id,0) AS value_as_concept_id, 
     clin.obsclin_result_num AS value_as_number, 
     coalesce(clin.obsclin_result_text,
                 clin.obsclin_result_qual,
                 clin.obsclin_result_num::text) AS value_source_value,
    vo.visit_occurrence_id AS visit_occurrence_id, 
     'SITE' as site
FROM SITE_pcornet.obs_clin clin
inner join SITE_pedsnet.person person on clin.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on clin.encounterid=vo.visit_source_value
left join lnc on clin.obsclin_code=lnc.concept_code
left join unit on clin.obsclin_result_unit=unit.concept_code
left join qual on clin.obsclin_result_qual=qual.concept_code
where obsclin_type='SM';

commit;