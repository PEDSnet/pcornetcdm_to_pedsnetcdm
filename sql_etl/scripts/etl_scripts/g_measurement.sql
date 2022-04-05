-- /* sequence is needed as the vital need to be transpose one id multiple values */
create sequence if not exists SITE_pedsnet.measurement_id_seq;

-- -- create index if not exists lab_result_cm_idx on SITE_pcornet.lab_result_cm (lab_result_cm_id);

begin;

-- pulling data from vitals ht
create table if not exists SITE_pedsnet.meas_vital_ht as
SELECT distinct
     3023540 AS measurement_concept_id, 
     v_ht.measure_date AS measurement_date, 
     (v_ht.measure_date || ' '|| v_ht.measure_time)::timestamp AS measurement_datetime, 
     null as measurement_order_date, 
     null as measurement_order_datetime, 
     null as measurement_result_date, 
     null as measurement_result_datetime, 
     0 AS measurement_source_concept_id,
     'Height in cm (converted from inches)' AS measurement_source_value, 
     2000000033 as measurement_type_concept_id, 
     4172703 AS operator_concept_id, 
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
     8582 AS unit_concept_id,
     9237 AS unit_source_concept_id, 
     'cm (converted from inches)' AS unit_source_value, 
     NULL AS value_as_concept_id, 
     (v_ht.ht*2.54) AS value_as_number, 
     v_ht.ht AS value_source_value,
     vo.visit_occurrence_id AS visit_occurrence_id, 
     'SITE' as site
FROM SITE_pcornet.vital v_ht
left join SITE_pcornet.encounter enc on enc.encounterid = v_ht.encounterid
inner join SITE_pedsnet.person person on v_ht.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on v_ht.encounterid=vo.visit_source_value
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_ht.vital_source
where v_ht.ht is not null;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id,unit_source_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, site
from SITE_pedsnet.meas_vital_ht;

commit;

-- pulling data from vitals wt
create table if not exists SITE_pedsnet.meas_vital_wt as
SELECT
     3013762 AS measurement_concept_id, 
     v_wt.measure_date AS measurement_date, 
     (v_wt.measure_date || ' '|| v_wt.measure_time)::timestamp AS measurement_datetime, 
     null as measurement_order_date, 
     null as measurement_order_datetime, 
     null as measurement_result_date, 
     null as measurement_result_datetime, 
     0 AS measurement_source_concept_id,
     'Weight (converted from pounds)' AS measurement_source_value, 
     2000000033 as measurement_type_concept_id, 
     4172703 AS operator_concept_id, 
     person.person_id AS person_id, 
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
     9529 AS unit_concept_id, 
     8739 AS unit_source_concept_id, 
     'kg (converted from pounds)' AS unit_source_value, 
     NULL AS value_as_concept_id, 
     (v_wt.wt*0.453592) AS value_as_number, 
     v_wt.wt AS value_source_value,
     vo.visit_occurrence_id AS visit_occurrence_id, 
     'SITE' as site
FROM SITE_pcornet.vital v_wt
left join SITE_pcornet.encounter enc on enc.encounterid = v_wt.encounterid
inner join SITE_pedsnet.person person on v_wt.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on v_wt.encounterid=vo.visit_source_value
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_wt.vital_source
where v_wt.wt is not null;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, site
from SITE_pedsnet.meas_vital_wt;

commit;

-- puling the systolica data
create table if not exists SITE_pedsnet.meas_vital_sys as
SELECT
     coalesce(sys_con.source_concept_id::int, 3004249) AS measurement_concept_id, 
     v_sys.measure_date AS measurement_date, 
     (v_sys.measure_date || ' '|| v_sys.measure_time)::timestamp AS measurement_datetime,
     null as measurement_order_date, 
     null as measurement_order_datetime, 
     null as measurement_result_date, 
     null as measurement_result_datetime, 
     0 AS measurement_source_concept_id,
     v_sys.raw_systolic AS measurement_source_value, 
     2000000033 as measurement_type_concept_id, 
     4172703 AS operator_concept_id, 
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
     8876 AS unit_concept_id,
     0 AS unit_source_concept_id, 
     'millimeter mercury column' AS unit_source_value, 
     NULL AS value_as_concept_id, 
     v_sys.systolic AS value_as_number, 
     v_sys.raw_systolic AS value_source_value,
     vo.visit_occurrence_id AS visit_occurrence_id, 
     'SITE' as site
FROM SITE_pcornet.vital v_sys
left join SITE_pcornet.encounter enc on enc.encounterid = v_sys.encounterid
inner join SITE_pedsnet.person person on v_sys.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on v_sys.encounterid=vo.visit_source_value
left join 
     (
          select target_concept, source_concept_id
          from pcornet_maps.pedsnet_pcornet_valueset_map 
          where source_concept_class='BP Position'
          and not(concept_description like '%Diastolic%')
     ) as sys_con on sys_con.target_concept = v_sys.bp_position
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_sys.vital_source
where v_sys.systolic is not null;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, site
from SITE_pedsnet.meas_vital_sys;

commit;

-- puling the dystolic data
create table if not exists SITE_pedsnet.meas_vital_dia as
SELECT
     3012888 AS measurement_concept_id, 
     v_dia.measure_date AS measurement_date, 
     (v_dia.measure_date || ' '|| v_dia.measure_time)::timestamp AS measurement_datetime, 
     null as measurement_order_date, 
     null as measurement_order_datetime, 
     null as measurement_result_date, 
     null as measurement_result_datetime, 
     coalesce(dia_con.source_concept_id::int, 3012888) AS measurement_source_concept_id,
     v_dia.raw_diastolic AS measurement_source_value, 
     2000000033 as measurement_type_concept_id, 
     4172703 AS operator_concept_id, 
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
     8876 AS unit_concept_id,
     0 AS unit_source_concept_id, 
     'millimeter mercury column' AS unit_source_value, 
     NULL AS value_as_concept_id, 
     v_dia.diastolic AS value_as_number, 
     dia_con.concept_description AS value_source_value,
     vo.visit_occurrence_id AS visit_occurrence_id, 
     'SITE' as site
FROM SITE_pcornet.vital v_dia
left join SITE_pcornet.encounter enc on enc.encounterid = v_dia.encounterid
inner join SITE_pedsnet.person person on v_dia.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on v_dia.encounterid=vo.visit_source_value
left join 
     (
          select target_concept, source_concept_id, concept_description
          from pcornet_maps.pedsnet_pcornet_valueset_map 
          where source_concept_class='BP Position'
          and not(concept_description like '%Systolic%')
     ) as dia_con on dia_con.target_concept = v_dia.bp_position
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_dia.vital_source
where v_dia.diastolic is not null ;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, site
from SITE_pedsnet.meas_vital_dia;

commit;

-- pulling the original_bmi data
Create table if not exists SITE_pedsnet.meas_vital_bmi as
SELECT
3038553 AS measurement_concept_id, 
v_bmi.measure_date AS measurement_date, 
(v_bmi.measure_date || ' '|| v_bmi.measure_time)::timestamp AS measurement_datetime, 
null as measurement_order_date, 
null as measurement_order_datetime, 
null as measurement_result_date, 
null as measurement_result_datetime, 
0 AS measurement_source_concept_id,
'Original BMI' AS measurement_source_value, 
2000000033 as measurement_type_concept_id, 
4172703 AS operator_concept_id, 
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
null AS unit_concept_id, 
null as unit_source_concept_id,
null AS unit_source_value, 
NULL AS value_as_concept_id, 
v_bmi.original_bmi AS value_as_number, 
v_bmi.original_bmi AS value_source_value,
vo.visit_occurrence_id AS visit_occurrence_id, 
'SITE' as site
FROM SITE_pcornet.vital v_bmi
left join SITE_pcornet.encounter enc on enc.encounterid = v_bmi.encounterid
inner join SITE_pedsnet.person person on v_bmi.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
     on v_bmi.encounterid=vo.visit_source_value
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_bmi.vital_source
where v_bmi.original_bmi is not null ;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id::bigint, unit_source_concept_id::bigint, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, site
from SITE_pedsnet.meas_vital_bmi;

commit;
-- pulling lab data from lab_result_cm
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
select distinct
     nextval('SITE_pedsnet.measurement_id_seq')::bigint AS measurement_id,
     coalesce(c.concept_id,0)  as measurement_concept_id,
     coalesce(lab.result_date,lab.specimen_date) as measurement_date, 
     coalesce(lab.result_date,lab.specimen_date)::timestamp as measurement_datetime,
     lab.lab_order_date as measurement_order_date,
     lab.lab_order_date::timestamp as measurement_order_datetime,
     lab.result_date as measurement_result_date, 
     lab.result_date::timestamp as measurement_result_datetime,
     coalesce(c.concept_id,0) as measurement_source_concept_id,
     lab.LAB_LOINC as measurement_source_value,
     44818702 AS measurement_type_concept_id,
     coalesce(
          case
               when lab.result_modifier = 'OT' then 4172703
               else opa.source_concept_id::int 
          end,0) as operator_concept_id,
     person.person_id AS person_id,  
     coalesce(priority.source_concept_id::int,44814650) as priority_concept_id,
     null as priority_source_value,
     vo.provider_id as provider_id,
     lab.norm_range_high::numeric as range_high,
     coalesce(
		case 
			when hi_mod.source_concept_id = '[TBD]' then 0
			else hi_mod.source_concept_id::int
		end,0) as range_high_operator_concept_id, 
     null as range_high_source_value,
     lab.norm_range_low::numeric as range_low,
     coalesce(
		case 
			when lo_mod.source_concept_id = '[TBD]' then 0
			else lo_mod.source_concept_id::int
		end,0) as range_low_operator_concept_id, 
     null as range_low_source_value,
     coalesce(case
          when specimen_source = 'SPECIMEN' then 4048506
          when specimen_source = 'BODY_FLD' then 4204181
          when specimen_source = 'TISSUE' then 4002890
          when specimen_source = 'XXX.SWAB' then 4120698
          when specimen_source = 'URINE' then 4046280
          when specimen_source = 'BLD' then 4001225
          when specimen_source = 'CVX_VAG' then 45765695
          when specimen_source = 'PLAS' then 4000626
          when specimen_source = 'BONE_MARROW' then 4000623
          when specimen_source = 'SKIN' then 43531265
          when specimen_source = 'BREAST' then 4132242
          when specimen_source = 'SER' then 4001181
          when specimen_source = 'CSF' then 4124259
          when specimen_source = 'STOOL' then 4002879
          when specimen_source = 'COLON' then 4002892
          when specimen_source = 'MILK' then 4001058
          when specimen_source = 'LYMPH_NODE' then 4124291
          when specimen_source = 'THYROID' then 4164619
          when specimen_source = 'PENIS' then 4002896
          when specimen_source = 'CALCULUS' then 4001065
          when specimen_source = 'LUNG' then 4133172
          when specimen_source = 'ANAL' then 4002895
          when specimen_source = 'TISS.FNA' then 4046275
          when specimen_source = 'BONE' then 4328578
          when specimen_source = 'BRONCHIAL' then 4001187
          when specimen_source = 'SALIVARY_GLAND.FNA' then 4265164
          when specimen_source = 'SALIVA' then 4001062
          when specimen_source = 'PROSTATE' then 4001186
          when specimen_source = 'RESPIRATORY.LOWER' then 4119538
          when specimen_source = 'RESPIRATORY.UPPER' then 4119537
          when specimen_source = 'GENITAL' then 4001063
          when specimen_source = 'LIVER.FNA' then 4335802
          when specimen_source = 'EAR' then 4204951
          when specimen_source = 'LYMPH_NODE.FNA' then 4333886
          when specimen_source = 'EYE' then 4001190
          when specimen_source = 'ABSCESS' then 4001183
          when specimen_source = 'PLACENTA' then 4001192
          when specimen_source = 'PANCREAS' then 4133175
          when specimen_source = 'SINUS' then 4332520
          when specimen_source = 'LIVER' then 4002224
          when specimen_source = 'KIDNEY.FNA' then 4048981
          when specimen_source = 'PROSTATE.FNA' then 40480935
          when specimen_source = 'THYROID.FNA' then 4204318
          when specimen_source = 'KIDNEY' then 4133742
          when specimen_source = 'OVARY' then 4027387
          when specimen_source = 'RESPIRATORY' then 4119536
          when specimen_source = 'BLDC' then 4046834
          when specimen_source = 'BLDV' then 4045667
          when specimen_source = 'PPP' then 4000627
          when specimen_source = 'RBC' then 4000622
          when specimen_source = 'SER_PLAS' then 4000626
          when specimen_source = 'THRT' then 4002893
          when specimen_source = 'URINE_SED' then 4045758
          when specimen_source = 'OT' then 44814649
     end,spec_con.source_concept_id::int,0) as specimen_concept_id, 
     lab.specimen_source as specimen_source_value, 
     coalesce(
          case 
               when lab.result_unit = '[ppm]' then 9387
               when lab.result_unit = '%{activity}' then 8687
               when lab.result_unit = 'nmol/min/mL' then 44777635
               when lab.result_unit = 'kU/L' then 8810
               else units.source_concept_id::int
          end, 0) as unit_concept_id,
     coalesce(
          case 
               when lab.result_unit = '[ppm]' then 9387
               when lab.result_unit = '%{activity}' then 8687
               when lab.result_unit = 'nmol/min/mL' then 44777635
               when lab.result_unit = 'kU/L' then 8810
               else units.source_concept_id::int
          end, 0) as unit_source_concept_id,  
     lab.raw_unit as unit_source_value, 
     case 
		when lower(trim(result_qual)) = 'positive' then 45884084
	     when lower(trim(result_qual)) = 'negative' then 45878583
          when lower(trim(result_qual)) = 'pos' then 45884084
          when lower(trim(result_qual)) = 'neg' then 45878583
          when lower(trim(result_qual)) = 'presumptive positive' then 45884084
          when lower(trim(result_qual)) = 'presumptive negative' then 45878583
          when lower(trim(result_qual)) = 'detected' then 45884084
          when lower(trim(result_qual)) = 'not detected' then 45878583
          when lower(trim(result_qual)) = 'inconclusive' then 45877990
          when lower(trim(result_qual)) = 'normal' then 45884153
          when lower(trim(result_qual)) = 'abnormal' then 45878745
          when lower(trim(result_qual)) = 'low' then 45881666
          when lower(trim(result_qual)) = 'high' then 45876384
          when lower(trim(result_qual)) = 'borderline' then 45880922
          when lower(trim(result_qual)) = 'elevated' then 4328749  --ssh add issue number 55 - 6/26/2020 
          when lower(trim(result_qual)) = 'undetermined' then 45880649
          when lower(trim(result_qual)) = 'undetectable' then 45878583 
          when lower(trim(result_qual)) = 'un' then 0
          when lower(trim(result_qual)) = 'unknown' then 0
          when lower(trim(result_qual)) = 'no information' then 46237210
          else 45877393 
     end as value_as_concept_id,
     lab.result_num as value_as_number, 
     COALESCE(NULLIF(lab.result_num, 0)::text, lab.raw_result, 'Unknown') as value_source_value,
     vo.visit_occurrence_id as visit_occurrence_id,    
     'SITE' as site
from SITE_pcornet.LAB_RESULT_CM as lab
inner join SITE_pedsnet.person person on lab.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo on lab.encounterid=vo.visit_source_value
left join 
     (
          select target_concept, source_concept_id
          from pcornet_maps.pedsnet_pcornet_valueset_map 
          where source_concept_class = 'Result modifier'
          and not (target_concept = 'OT' and source_concept_id = '0')
     ) as opa on opa.target_concept = lab.result_modifier
left join 
     (
          select target_concept, source_concept_id
          from pcornet_maps.pedsnet_pcornet_valueset_map 
          where source_concept_class = 'Result modifier'
          and not (target_concept = 'OT' and source_concept_id = '0')
     ) as hi_mod on hi_mod.target_concept = lab.NORM_MODIFIER_HIGH
left join 
     (
          select target_concept, source_concept_id
          from pcornet_maps.pedsnet_pcornet_valueset_map 
          where source_concept_class = 'Result modifier'
          and not (target_concept = 'OT' and source_concept_id = '0')
     ) as lo_mod on lo_mod.target_concept = lab.NORM_MODIFIER_LOW
	left join 
     (
          select target_concept, source_concept_id
          from pcornet_maps.pedsnet_pcornet_valueset_map
          where source_concept_class = 'Result unit'
		  and source_concept_id <> 'null'
          and not (target_concept = '10*3/uL' and pcornet_name is null)
          and not (target_concept = '10*6/uL' and pcornet_name is null)
          and not (target_concept = 'a' and concept_description = 'y | year')
          and not (target_concept = '[APL''U]/mL' and pcornet_name is null)
          and not (target_concept = '{breaths}/min' and concept_description = 'breaths/min')
          and not (target_concept = '{cells}/[HPF]' and concept_description <> 'cells per high power field')
          and not (target_concept = '{cells}/uL' and concept_description = 'cells/cumm')
          and not (target_concept = '[GPL''U]/mL' and pcornet_name is null)
          and not (target_concept = '{index_val}' and concept_description <> 'index value')
          and not (target_concept = '[IU]/g{Hb}' and pcornet_name is null)
          and not (target_concept = 'k[IU]/L' and concept_description = 'kilo-international unit per liter')
          and not (target_concept = 'meq/L' and pcornet_name is null)
          and not (target_concept = 'mmol/mol{creat}' and concept_description = 'mmol/mol cr')
          and not (target_concept = '[MPL''U]/mL' and pcornet_name is null)
          and not (target_concept = 'NI' and source_concept_id = '0')
          and not (target_concept = '%{normal}' and concept_description = 'NEG>CULTURE')
          and not (target_concept = '[pH]' and pcornet_name is null)
          and not (target_concept = '{ratio}' and pcornet_name is null)
          and not (target_concept = 'ug{FEU}/mL' and pcornet_name is null)
          and not (target_concept = 'U/g{Hb}' and pcornet_name is null) 
          and not (target_concept = '/uL' and concept_description = '/cumm')
          and not (target_concept = 'U/mL' and pcornet_name is null)
     ) as units on lab.result_unit = units.target_concept
left join pcornet_maps.pedsnet_pcornet_valueset_map priority on priority.target_concept = lab.priority and priority.source_concept_class = 'Lab priority'
left join vocabulary.concept c on lab.lab_loinc=c.concept_code and c.vocabulary_id='LOINC'
left join  
     (
          select target_concept, source_concept_id
          from pcornet_maps.pedsnet_pcornet_valueset_map
          where source_concept_class = 'Specimen concept'
          and not(target_concept = 'ABSCESS' and source_concept_id <> '4001183')
          and not(target_concept = 'ANAL' and source_concept_id <> '4002895')
          and not(target_concept = 'BLD' and source_concept_id <> '4001225')
          and not(target_concept = 'BODY_FLD' and source_concept_id <> '4204181')
          and not(target_concept = 'BONE' and source_concept_id <> '4328578')
          and not(target_concept = 'BONE_MARROW' and source_concept_id <> '4000623')
          and not(target_concept = 'BREAST' and source_concept_id <> '4132242')
          and not(target_concept = 'BRONCHIAL' and source_concept_id <> '4001187')
          and not(target_concept = 'CALCULUS' and source_concept_id <> '4001065')
          and not(target_concept = 'COLON' and source_concept_id <> '4002892')
          and not(target_concept = 'CSF' and source_concept_id <> '4124259')
          and not(target_concept = 'CVX_VAG' and source_concept_id <> '45765695')
          and not(target_concept = 'EAR' and source_concept_id <> '4204951')
          and not(target_concept = 'EYE' and source_concept_id <> '4001190')
          and not(target_concept = 'GENITAL' and source_concept_id <> '4001063')
          and not(target_concept = 'KIDNEY' and source_concept_id <> '4133742')
          and not(target_concept = 'KIDNEY.FNA' and source_concept_id <> '4048981')
          and not(target_concept = 'LIVER' and source_concept_id <> '4002224')
          and not(target_concept = 'LIVER.FNA' and source_concept_id <> '4335802')
          and not(target_concept = 'LUNG' and source_concept_id <> '4133172')
          and not(target_concept = 'LYMPH_NODE' and source_concept_id <> '4124291')
          and not(target_concept = 'LYMPH_NODE.FNA' and source_concept_id <> '4333886')
          and not(target_concept = 'MILK' and source_concept_id <> '4001058')
          and not(target_concept = 'OVARY' and source_concept_id <> '4027387')
          and not(target_concept = 'PANCREAS' and source_concept_id <> '4133175')
          and not(target_concept = 'PENIS' and source_concept_id <> '4002896')
          and not(target_concept = 'PLACENTA' and source_concept_id <> '4001192')
          and not(target_concept = 'PLAS' and source_concept_id <> '4000626')
          and not(target_concept = 'PROSTATE' and source_concept_id <> '4001186')
          and not(target_concept = 'PROSTATE.FNA' and source_concept_id <> '40480935')
          and not(target_concept = 'RESPIRATORY' and source_concept_id <> '4119536')
          and not(target_concept = 'RESPIRATORY.LOWER' and source_concept_id <> '4119538')
          and not(target_concept = 'RESPIRATORY.UPPER' and source_concept_id <> '4119537')
          and not(target_concept = 'SALIVA' and source_concept_id <> '4001062')
          and not(target_concept = 'SALIVARY_GLAND.FNA' and source_concept_id <> '4265164')
          and not(target_concept = 'SER' and source_concept_id <> '4001181')
          and not(target_concept = 'SINUS' and source_concept_id <> '4332520')
          and not(target_concept = 'SKIN' and source_concept_id <> '43531265')
          and not(target_concept = 'SPECIMEN' and source_concept_id <> '4048506')
          and not(target_concept = 'STOOL' and source_concept_id <> '4002879')
          and not(target_concept = 'THYROID' and source_concept_id <> '4164619')
          and not(target_concept = 'THYROID.FNA' and source_concept_id <> '4204318')
          and not(target_concept = 'TISS.FNA' and source_concept_id <> '4046275')
          and not(target_concept = 'TISSUE' and source_concept_id <> '4002890')
          and not(target_concept = 'URINE' and source_concept_id <> '4046280')
          and not(target_concept = 'XXX.SWAB' and source_concept_id <> '4120698')
     ) as spec_con on spec_con.target_concept = lab.specimen_source;

commit;

 -- pulling LOINC data from obs_clin
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
     null::date as measurement_order_date, 
     null::timestamp as measurement_order_datetime, 
     null::date as measurement_result_date, 
     null::timestamp as measurement_result_datetime, 
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
     null::int as range_high, 
     null::int as range_high_operator_concept_id, 
     null as range_high_source_value, 
     null::int as range_low, 
     null::int as range_low_operator_concept_id, 
     null as range_low_source_value, 
     null::int as specimen_concept_id, 
     null as specimen_source_value, 
     coalesce(
          case 
               when clin.obsclin_result_unit = 'NI' then 444814650
               else unit.concept_id
          end,0) AS unit_concept_id,
     coalesce(
          case 
               when clin.obsclin_result_unit = 'NI' then 444814650
               else unit.concept_id
          end,0) AS unit_source_concept_id,
     coalesce(clin.obsclin_result_unit,clin.raw_obsclin_unit) AS unit_source_value, 
     coalesce(
          case
               when clin.obsclin_result_qual = 'NI' then 444814650
               when clin.obsclin_result_qual = 'OT' then 44814649
               else qual.concept_id
          end,0) AS value_as_concept_id, 
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

 -- pulling SNOMED data from obs_clin
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
     null::date as measurement_order_date, 
     null::timestamp as measurement_order_datetime, 
     null::date as measurement_result_date, 
     null::timestamp as measurement_result_datetime, 
      coalesce(sm.concept_id,0) AS measurement_source_concept_id,
     coalesce(clin.raw_obsclin_name,sm.concept_name) AS measurement_source_value, 
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
     null::int as range_high, 
     null::int as range_high_operator_concept_id, 
     null as range_high_source_value, 
     null::int as range_low, 
     null::int as range_low_operator_concept_id, 
     null as range_low_source_value, 
     null::int as specimen_concept_id, 
     null as specimen_source_value, 
     coalesce(
          case 
               when clin.obsclin_result_unit = 'NI' then 444814650
               else unit.concept_id
          end,0) AS unit_concept_id,
     coalesce(
          case 
               when clin.obsclin_result_unit = 'NI' then 444814650
               else unit.concept_id
          end,0) AS unit_source_concept_id,
     coalesce(clin.obsclin_result_unit,clin.raw_obsclin_unit) AS unit_source_value, 
     coalesce(
          case
               when clin.obsclin_result_qual = 'NI' then 444814650
               when clin.obsclin_result_qual = 'OT' then 44814649
               else qual.concept_id
          end,0) AS value_as_concept_id, 
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
left join sm on clin.obsclin_code=sm.concept_code
left join unit on clin.obsclin_result_unit=unit.concept_code
left join qual on clin.obsclin_result_qual=qual.concept_code
where obsclin_type='SM';

commit;