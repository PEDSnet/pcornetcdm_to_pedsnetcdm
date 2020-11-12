begin;
INSERT INTO SITE_pedsnet.condition_occurrence(condition_concept_id, condition_end_date, condition_end_datetime,
condition_occurrence_id, condition_source_concept_id, condition_source_value, condition_start_date, condition_start_datetime, 
condition_status_concept_id, condition_status_source_value, condition_type_concept_id, person_id, poa_concept_id, 
provider_id, stop_reason, visit_occurrence_id, domain_source, site)
SELECT coalesce(xw.target_concept_id,0) as condition_concept_id,
cond.resolve_date as condition_end_date, 
cond.resolve_date::timestamp as condition_end_datetime,
trim(LEADING 'c' from cond.conditionid)::int AS condition_occurrence_id,
xw.source_code_concept_id as condition_source_concept_id,
cond.raw_CONDITION AS condition_source_value,
cond.report_date as condition_start_date, 
cond.report_date::timestamp as condition_start_datetime,
cst.TARGET_CONCEPT_ID AS CONDITION_STATUS_CONCEPT_ID,
cond.raw_CONDITION_STATUS AS condition_status_source_value,
-- coalesce(typ.source_concept_id::int, 32817) 
32817as condition_type_concept_id,
cond.patid::int AS person_id,   
null as poa_concept_id, 
enc.providerid::int as provider_id,   
NULL as stop_reason,    
cond.encounterid::int as visit_occurrence_id, 
'PCORNET_CONDITION' as DOMAIN_SOURCE,
'SITE' as site
FROM SITE_pcornet.CONDITION cond
left join SITE_pcornet.encounter enc on enc.encounterid = cond.encounterid
LEFT JOIN cdmh_staging.p2o_term_xwalk csrc on csrc.CDM_TBL='CONDITION' AND csrc.cdm_tbl_column_name='CONDITION_SOURCE' AND csrc.src_code = cond.CONDITION_SOURCE
LEFT JOIN cdmh_staging.p2o_term_xwalk cst on cst.CDM_TBL='CONDITION' AND cst.cdm_tbl_column_name='CONDITION_STATUS' AND cst.src_code = cond.CONDITION_STATUS
left JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on cond.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Condition'
                                                                              --and xw.target_concept_id = mp.target_concept_id
                                                                              and Xw.Src_Code_Type = cond.condition_type
left join pcornet_maps.pedsnet_pcornet_valueset_map typ on typ.target_concept = cond.
-- where substring(conditionid,1,1) = 'c'--  right now we are using the prefix in the condition table testing in this
;
commit;
*/

