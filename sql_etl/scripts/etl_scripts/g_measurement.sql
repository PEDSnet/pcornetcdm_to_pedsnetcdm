/* sequence is needed as the vital need to be transpose one id multiple values */
create sequence if not exists SITE_pedsnet.measurement_id_seq;

-- create index if not exists lab_result_cm_idx on SITE_pcornet.lab_result_cm (lab_result_cm_id);

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
     NULL AS operator_concept_id, 
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
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id,unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
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
     NULL AS operator_concept_id, 
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
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
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
     NULL AS operator_concept_id, 
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
left join pcornet_maps.pedsnet_pcornet_valueset_map sys_con on sys_con.target_concept = v_sys.bp_position
	                                                            AND sys_con.source_concept_class='BP Position'
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_sys.vital_source
where v_sys.systolic is not null;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
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
     NULL AS operator_concept_id, 
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
left join pcornet_maps.pedsnet_pcornet_valueset_map dia_con on dia_con.target_concept = v_dia.bp_position
	                                                            AND dia_con.source_concept_class='BP Position'
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_dia.vital_source
where v_dia.diastolic is not null ;

commit;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
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
NULL AS operator_concept_id, 
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
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('SITE_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id::bigint, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, site
from SITE_pedsnet.meas_vital_bmi;

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
     unit_source_value, 
     value_as_concept_id, 
     value_as_number, 
     value_source_value,
     visit_occurrence_id, 
     site)
with lab as (
select distinct
	lab_result_cm_id,
	coalesce(lab.result_date,lab.specimen_date) as measurement_date, 
	coalesce(lab.result_date,lab.specimen_date)::timestamp as measurement_datetime,
	lab.lab_order_date as measurement_order_date,
	lab.lab_order_date::timestamp as measurement_order_datetime,
	lab.result_date as measurement_result_date, 
	lab.result_date::timestamp as measurement_result_datetime,
	lab.LAB_LOINC as measuremnt_source_value,
	lab.norm_range_high::numeric as range_high,
	lab.norm_range_low::numeric as range_low,
	lab.specimen_source as specimen_source_value, 
	lab.result_num as value_as_number, 
	COALESCE(NULLIF(lab.result_num, 0)::text, lab.raw_result) as value_source_value,
	person.person_id AS person_id,
	vo.provider_id as provider_id,
	vo.visit_occurrence_id as visit_occurrence_id,
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
	lab.result_modifier,
	lab.norm_modifier_high,
	lab.norm_modifier_low,
	lab.priority,
	lab.specimen_source,
	lab.result_unit,
	lab.lab_loinc
from SITE_pcornet.LAB_RESULT_CM as lab
inner join SITE_pedsnet.person person on lab.patid=person.person_source_value
left join SITE_pedsnet.visit_occurrence vo on lab.encounterid=vo.visit_source_value
-- where lab.result_date >= '2021-01-01'
),

map1 as (
select distinct
	lab_result_cm_id,
	coalesce(opa.source_concept_id::int,0) as operator_concept_id,
	coalesce(
		case 
			when hi_mod.source_concept_id = '[TBD]' then 0
			else hi_mod.source_concept_id::int
		end,0) as range_high_operator_concept_id, 
	coalesce(
		case 
			when lo_mod.source_concept_id = '[TBD]' then 0
			else lo_mod.source_concept_id::int
		end,0) as range_low_operator_concept_id, 
	coalesce(priority.source_concept_id::int,0) as priority_concept_id,
     coalesce(units.source_concept_id::int, 0) as unit_concept_id, 
	coalesce(c.concept_id,0)  as measurement_concept_id,
	coalesce(c.concept_id,0) as measurement_source_concept_id
FROM lab
left join pcornet_maps.pedsnet_pcornet_valueset_map opa on opa.target_concept = lab.result_modifier and opa.source_concept_class = 'Result modifier'
left join pcornet_maps.pedsnet_pcornet_valueset_map hi_mod on hi_mod.target_concept = lab.norm_modifier_high and hi_mod.source_concept_class = 'Result modifier'
left join pcornet_maps.pedsnet_pcornet_valueset_map lo_mod on lo_mod.target_concept = lab.norm_modifier_low and lo_mod.source_concept_class = 'Result modifier'
left join pcornet_maps.pedsnet_pcornet_valueset_map priority on priority.target_concept = lab.priority and priority.source_concept_class = 'Lab priority'
left join pcornet_maps.pedsnet_pcornet_valueset_map units on lab.result_unit = units.target_concept and units.source_concept_class = 'Result unit'
left join vocabulary.concept c on lab.lab_loinc=c.concept_code and c.vocabulary_id='LOINC'
),

map2 as (
select distinct
	lab_result_cm_id,
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
     end,0) as specimen_concept_id
FROM lab
where specimen_source in ('SPECIMEN','BODY_FLD','TISSUE','XXX.SWAB','URINE','BLD','CVX_VAG','PLAS',
     'BONE_MARROW','SKIN','BREAST','SER','CSF','STOOL','COLON','MILK','LYMPH_NODE','THYROID','PENIS','CALCULUS',
     'LUNG','ANAL','TISS.FNA','BONE','BRONCHIAL','SALIVARY_GLAND.FNA','SALIVA','PROSTATE','RESPIRATORY.LOWER',
     'RESPIRATORY.UPPER','GENITAL','LIVER.FNA','EAR','LYMPH_NODE.FNA','EYE','ABSCESS','PLACENTA','PANCREAS','SINUS',
     'LIVER','KIDNEY.FNA','PROSTATE.FNA','THYROID.FNA','KIDNEY','OVARY','RESPIRATORY')
union
select distinct
	lab_result_cm_id,
	coalesce(spec_con.source_concept_id::int, 0) as specimen_concept_id
FROM 
	(select 
	 	lab_result_cm_id,
	 	specimen_source
	 from lab
	 where specimen_source not in ('SPECIMEN','BODY_FLD','TISSUE','XXX.SWAB','URINE','BLD','CVX_VAG','PLAS',
     'BONE_MARROW','SKIN','BREAST','SER','CSF','STOOL','COLON','MILK','LYMPH_NODE','THYROID','PENIS','CALCULUS',
     'LUNG','ANAL','TISS.FNA','BONE','BRONCHIAL','SALIVARY_GLAND.FNA','SALIVA','PROSTATE','RESPIRATORY.LOWER',
     'RESPIRATORY.UPPER','GENITAL','LIVER.FNA','EAR','LYMPH_NODE.FNA','EYE','ABSCESS','PLACENTA','PANCREAS','SINUS',
     'LIVER','KIDNEY.FNA','PROSTATE.FNA','THYROID.FNA','KIDNEY','OVARY','RESPIRATORY')
	) as lab
left join pcornet_maps.pedsnet_pcornet_valueset_map spec_con on spec_con.target_concept = lab.specimen_source 
     and spec_con.source_concept_class = 'Specimen concept'
)

SELECT distinct
     nextval('SITE_pedsnet.measurement_id_seq')::bigint AS measurement_id,
     map1.measurement_concept_id  as measurement_concept_id,
     lab.measurement_date as measurement_date, 
     lab.measurement_datetime as measurement_datetime,
     lab.measurement_order_date as measurement_order_date,
     lab.measurement_order_datetime as measurement_order_datetime,
     lab.measurement_result_date as measurement_result_date, 
     lab.measurement_result_datetime as measurement_result_datetime,
     map1.measurement_source_concept_id as measurement_source_concept_id,
     lab.measuremnt_source_value as measuremnt_source_value,
     44818702 AS measurement_type_concept_id,
     map1.operator_concept_id as operator_concept_id,
     lab.person_id AS person_id,  
     map1.priority_concept_id as priority_concept_id,
     null as priority_source_value,
     lab.provider_id as provider_id,
     lab.range_high as range_high,
     map1.range_high_operator_concept_id as range_high_operator_concept_id, 
     null as range_high_source_value,
     lab.range_low as range_low,
     map1.range_low_operator_concept_id as range_low_operator_concept_id, 
     null as range_low_source_value,
     map2.specimen_concept_id as specimen_concept_id, 
     lab.specimen_source_value as specimen_source_value, 
     map1.unit_concept_id as unit_concept_id, 
     lab.unit_source_value as unit_source_value, 
     lab.value_as_concept_id as value_as_concept_id,
     lab.value_as_number as value_as_number, 
     coalesce(lab.value_source_value, 'Unknown') as value_source_value,
     lab.visit_occurrence_id as visit_occurrence_id,    
     'SITE' as site
FROM lab
inner join map1 on map1.lab_result_cm_id = lab.lab_result_cm_id
inner join map2 on map2.lab_result_cm_id = lab.lab_result_cm_id;

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
     coalesce(unit.concept_id,0) AS unit_concept_id,
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
     coalesce(unit.concept_id,0) AS unit_concept_id,
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
left join sm on clin.obsclin_code=sm.concept_code
left join unit on clin.obsclin_result_unit=unit.concept_code
left join qual on clin.obsclin_result_qual=qual.concept_code
where obsclin_type='SM';

commit;