/* sequence is needed as the vital need to be transpose one id multiple values */
create sequence SITE_pedsnet.measurement_id_seq;

INSERT INTO SITE_pedsnet.measurement ( measurement_id,measurement_concept_id, measurement_date, measurement_datetime,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site)
SELECT distinct
lab.lab_result_cm_id::int AS measurement_id,
coalesce(xw.target_concept_id,0)  as measurement_concept_id,
lab.specimen_date as measurement_date, 
lab.specimen_date::timestamp as measurement_datetime,
lab.lab_order_date as measurement_order_date,
lab.lab_order_date::timestamp as measurement_order_datetime,
lab.result_date as measurement_result_date, 
lab.result_date::timestamp as measurement_result_datetime,
0 as MEASUREMENT_SOURCE_CONCEPT_ID,
lab.LAB_LOINC as MEASUREMENT_SOURCE_VALUE,
coalesce(xw2.target_concept_id,44818702) AS measurement_type_concept_id,
coalesce(opa.source_concept_id::int, 0) as OPERATOR_CONCEPT_ID,
lab.patid::int AS person_id,  
coalesce(priority.source_concept_id::int, 0) as priority_concept_id,
null as priority_source_value,
enc.providerid::int as PROVIDER_ID,
lab.norm_range_high::numeric as range_high,
coalesce(hi_mod.source_concept_id::int, 0) as range_high_operator_concept_id, 
null as range_high_source_value,
lab.norm_range_low::numeric as range_low,
coalesce(lo_mod.source_concept_id::int, 0) as range_low_operator_concept_id, 
null as range_low_source_value,
coalesce(spec_con.source_concept_id::int, 0) as specimen_concept_id, 
lab.specimen_source as specimen_source_value, 
coalesce(units.source_concept_id::int, u.target_concept_id, 0) as unit_concept_id, 
raw_unit as unit_source_value, 
case when lower(trim(result_qual)) = 'positive' then 45884084
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
end as VALUE_AS_CONCEPT_ID,
lab.result_num as VALUE_AS_NUMBER, 
COALESCE(NULLIF(lab.result_num, 0)::text, lab.raw_result) as VALUE_SOURCE_VALUE,            
lab.encounterid::int as VISIT_OCCURRENCE_ID,    
'PCORNET_LAB_RESULT_CM' AS DOMAIN_SOURCE,
'SITE' as site
FROM SITE_pcornet.LAB_RESULT_CM lab
left join SITE_pcornet.encounter enc on enc.encounterid = lab.encounterid
left join pcornet_maps.pedsnet_pcornet_valueset_map opa on opa.target_concept = lab.result_modifier and opa.source_concept_class = 'Result modifier'
left join pcornet_maps.pedsnet_pcornet_valueset_map hi_mod on hi_mod.target_concept = lab.norm_modifier_high and hi_mod.source_concept_class = 'Result modifier'
left join pcornet_maps.pedsnet_pcornet_valueset_map lo_mod on lo_mod.target_concept = lab.norm_modifier_low and lo_mod.source_concept_class = 'Result modifier'
left join pcornet_maps.pedsnet_pcornet_valueset_map priority on priority.target_concept = lab.priority and priority.source_concept_class = 'Lab priority'
left join pcornet_maps.pedsnet_pcornet_valueset_map spec_con on spec_con.target_concept = lab.specimen_source and spec_con.source_concept_class = 'Specimen concept'
left join pcornet_maps.pedsnet_pcornet_valueset_map units on lab.result_unit = units.target_concept and units.source_concept_class = 'Result unit'
LEFT JOIN CDMH_STAGING.p2o_medadmin_term_xwalk u on lab.result_unit = u.src_code and u.src_cdm_column = 'RX_DOSE_ORDERED_UNIT'
left JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on lab.lab_loinc = xw.src_code  and xw.CDM_TBL = 'LAB_RESULT_CM' AND xw.target_domain_id = 'Measurement' 
LEFT JOIN cdmh_staging.p2o_term_xwalk xw2 ON lab.LAB_RESULT_SOURCE = xw2.src_code AND xw2.cdm_tbl = 'LAB_RESULT_CM' AND xw2.cdm_tbl_column_name = 'LAB_RESULT_SOURCE'
;

-- pulling data from vitals ht
Create table SITE_pedsnet.meas_vital_ht as
SELECT distinct
3023540 AS measurement_concept_id, 
v_ht.measure_date AS measurement_date, 
(v_ht.measure_date || ' '|| v_ht.measure_time)::timestamp AS measurement_datetime, 
v_ht.vitalid::int as measurement_id, 
null as measurement_order_date, 
null as measurement_order_datetime, 
null as measurement_result_date, 
null as measurement_result_datetime, 
0 AS measurement_source_concept_id,
'Height in inches' AS measurement_source_value, 
2000000033 as measurement_type_concept_id, 
NULL AS operator_concept_id, 
v_ht.patid::int  AS person_id, 
0  as priority_concept_id, 
null as priority_source_value, 
enc.providerid::int as provider_id, 
null as range_high, 
null as range_high_operator_concept_id, 
null as range_high_source_value, 
null as range_low, 
null as range_low_operator_concept_id, 
null as range_low_source_value, 
null as specimen_concept_id, 
null as specimen_source_value, 
9327 AS unit_concept_id, 
'Inches' AS unit_source_value, 
NULL AS value_as_concept_id, 
v_ht.ht AS value_as_number, 
v_ht.ht AS value_source_value,
enc.encounterid::int AS visit_occurrence_id, 
'PCORNET_VITAL' AS domain_source,
'SITE' as site
FROM SITE_pcornet.vital v_ht
left join SITE_pcornet.encounter enc on enc.encounterid = v_ht.encounterid
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_ht.vital_source
where v_ht.ht is not null;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('pcornet_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site
from SITE_pedsnet.meas_vital_ht;

-- pulling data from vitals wt
Create table SITE_pedsnet.meas_vital_wt as
SELECT
3013762 AS measurement_concept_id, 
v_wt.measure_date AS measurement_date, 
(v_wt.measure_date || ' '|| v_wt.measure_time)::timestamp AS measurement_datetime, 
v_wt.vitalid::int as measurement_id, 
null as measurement_order_date, 
null as measurement_order_datetime, 
null as measurement_result_date, 
null as measurement_result_datetime, 
0 AS measurement_source_concept_id,
'Weight in pounds' AS measurement_source_value, 
2000000033 as measurement_type_concept_id, 
NULL AS operator_concept_id, 
v_wt.patid::int  AS person_id, 
0  as priority_concept_id, 
null as priority_source_value, 
enc.providerid::int as provider_id, 
null as range_high, 
null as range_high_operator_concept_id, 
null as range_high_source_value, 
null as range_low, 
null as range_low_operator_concept_id, 
null as range_low_source_value, 
null as specimen_concept_id, 
null as specimen_source_value, 
8739 AS unit_concept_id, 
'Pounds' AS unit_source_value, 
NULL AS value_as_concept_id, 
v_wt.wt AS value_as_number, 
v_wt.wt AS value_source_value,
enc.encounterid::int AS visit_occurrence_id, 
'PCORNET_VITAL' AS domain_source,
'SITE' as site
FROM SITE_pcornet.vital v_wt
left join SITE_pcornet.encounter enc on enc.encounterid = v_wt.encounterid
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_wt.vital_source
where v_wt.wt is not null;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('pcornet_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site
from SITE_pedsnet.meas_vital_wt;

-- puling the systolica data
create table SITE_pedsnet.meas_vital_sys as
SELECT
coalesce(sys_con.source_concept_id::int, 3004249) AS measurement_concept_id, 
v_sys.measure_date AS measurement_date, 
(v_sys.measure_date || ' '|| v_sys.measure_time)::timestamp AS measurement_datetime, 
v_sys.vitalid as measurment_id,
null as measurement_order_date, 
null as measurement_order_datetime, 
null as measurement_result_date, 
null as measurement_result_datetime, 
0 AS measurement_source_concept_id,
v_sys.raw_systolic AS measurement_source_value, 
2000000033 as measurement_type_concept_id, 
NULL AS operator_concept_id, 
v_sys.patid::int  AS person_id, 
0  as priority_concept_id, 
null as priority_source_value, 
enc.providerid::int as provider_id, 
null as range_high, 
null as range_high_operator_concept_id, 
null as range_high_source_value, 
null as range_low, 
null as range_low_operator_concept_id, 
null as range_low_source_value, 
null as specimen_concept_id, 
null as specimen_source_value, 
8876 AS unit_concept_id, 
'millimeter mercury column' AS unit_source_value, 
NULL AS value_as_concept_id, 
v_sys.systolic AS value_as_number, 
v_sys.raw_systolic AS value_source_value,
enc.encounterid::int AS visit_occurrence_id, 
'PCORNET_VITAL' AS domain_source,
'SITE' as site
FROM SITE_pcornet.vital v_sys
left join SITE_pcornet.encounter enc on enc.encounterid = v_sys.encounterid
left join pcornet_maps.pedsnet_pcornet_valueset_map sys_con on sys_con.target_concept = v_sys.bp_position
	                                                            AND sys_con.source_concept_class='BP Position'
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_sys.vital_source
where v_sys.systolic is not null;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('pcornet_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site
from SITE_pedsnet.meas_vital_sys;

-- puling the dystolic data
create table SITE_pedsnet.meas_vital_dia as
SELECT
3012888 AS measurement_concept_id, 
v_dia.measure_date AS measurement_date, 
(v_dia.measure_date || ' '|| v_dia.measure_time)::timestamp AS measurement_datetime, 
v_dia.vitalid as measurment_id,
null as measurement_order_date, 
null as measurement_order_datetime, 
null as measurement_result_date, 
null as measurement_result_datetime, 
coalesce(dia_con.source_concept_id::int, 3012888) AS measurement_source_concept_id,
v_dia.raw_diastolic AS measurement_source_value, 
2000000033 as measurement_type_concept_id, 
NULL AS operator_concept_id, 
v_dia.patid::int  AS person_id, 
0  as priority_concept_id, 
null as priority_source_value, 
enc.providerid::int as provider_id, 
null as range_high, 
null as range_high_operator_concept_id, 
null as range_high_source_value, 
null as range_low, 
null as range_low_operator_concept_id, 
null as range_low_source_value, 
null as specimen_concept_id, 
null as specimen_source_value, 
8876 AS unit_concept_id, 
'millimeter mercury column' AS unit_source_value, 
NULL AS value_as_concept_id, 
v_dia.diastolic AS value_as_number, 
dia_con.concept_description AS value_source_value,
enc.encounterid::int AS visit_occurrence_id, 
'PCORNET_VITAL' AS domain_source,
'SITE' as site
FROM SITE_pcornet.vital v_dia
left join SITE_pcornet.encounter enc on enc.encounterid = v_dia.encounterid
left join pcornet_maps.pedsnet_pcornet_valueset_map dia_con on dia_con.target_concept = v_dia.bp_position
	                                                            AND dia_con.source_concept_class='BP Position'
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_dia.vital_source
where v_dia.diastolic is not null ;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('pcornet_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site
from SITE_pedsnet.meas_vital_dia;

-- puling the original_bmi data
Create table SITE_pedsnet.meas_vital_bmi as
SELECT
3038553 AS measurement_concept_id, 
v_bmi.measure_date AS measurement_date, 
(v_bmi.measure_date || ' '|| v_bmi.measure_time)::timestamp AS measurement_datetime, 
v_bmi.vitalid::int as measurment_id,
null as measurement_order_date, 
null as measurement_order_datetime, 
null as measurement_result_date, 
null as measurement_result_datetime, 
0 AS measurement_source_concept_id,
'Original BMI' AS measurement_source_value, 
2000000033 as measurement_type_concept_id, 
NULL AS operator_concept_id, 
v_bmi.patid::int  AS person_id, 
0  as priority_concept_id, 
null as priority_source_value, 
enc.providerid::int as provider_id, 
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
enc.encounterid::int AS visit_occurrence_id, 
'PCORNET_VITAL' AS domain_source,
'SITE' as site
FROM SITE_pcornet.vital v_bmi
left join SITE_pcornet.encounter enc on enc.encounterid = v_bmi.encounterid
LEFT JOIN cdmh_staging.p2o_vital_term_xwalk   vt ON vt.src_cdm_tbl = 'VITAL' AND vt.src_cdm_column = 'VITAL_SOURCE' AND vt.src_code = v_bmi.vital_source
where v_bmi.original_bmi is not null ;

INSERT INTO SITE_pedsnet.measurement (measurement_concept_id, measurement_date, measurement_datetime, measurement_id,
measurement_order_date, measurement_order_datetime, measurement_result_date, measurement_result_datetime, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high, range_high_operator_concept_id, 
range_high_source_value, range_low, range_low_operator_concept_id, range_low_source_value, specimen_concept_id, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site)
select measurement_concept_id, measurement_date, measurement_datetime, nextval('pcornet_pedsnet.measurement_id_seq') as measurement_id, 
measurement_order_date::date, measurement_order_datetime::timestamp, measurement_result_date::date, measurement_result_datetime::timestamp, 
measurement_source_concept_id, measurement_source_value, measurement_type_concept_id, operator_concept_id::int, person_id, 
priority_concept_id, priority_source_value, provider_id, range_high::numeric, range_high_operator_concept_id::int, 
range_high_source_value, range_low::numeric, range_low_operator_concept_id::int, range_low_source_value, specimen_concept_id::int, 
specimen_source_value, unit_concept_id, unit_source_value, value_as_concept_id::int, value_as_number, value_source_value,
visit_occurrence_id, domain_source, site
from SITE_pedsnet.meas_vital_bmi;

