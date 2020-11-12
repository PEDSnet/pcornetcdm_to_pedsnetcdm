Begin;
-- adding condition information
INSERT INTO pcornet_pedsnet.procedure_occurrence (procedure_occurrence_id, modifier_concept_id, modifier_source_value, person_id, 
procedure_concept_id, procedure_date, procedure_datetime,  procedure_source_concept_id, 
procedure_source_value, procedure_type_concept_id, provider_id, quantity, visit_occurrence_id, domain_source, site)
SELECT distinct on (proc.proceduresid) proc.proceduresid::int AS PROCEDURE_OCCURRENCE_ID,
0 MODIFIER_CONCEPT_ID, -- need to create a cpt_concept_id table based on the source_code_concept id
xw.src_code_type as MODIFIER_SOURCE_VALUE,
proc.patid::int AS person_id,  
coalesce(xw.target_concept_id,0) as PROCEDURE_CONCEPT_ID,
cond.report_date as PROCEDURE_DATE,
cond.report_date::timestamp as PROCEDURE_DATETIME,
xw.source_code_concept_id as PROCEDURE_SOURCE_CONCEPT_ID,
cond.raw_condition as PROCEDURE_SOURCE_VALUE,
case when cond.condition_source = 'HC' then 38000245 --HC=Healthcare  problem  list/EHR problem list entry
      when cond.condition_source ='PR' then 45905770 --PR : patient reported
      when cond.condition_source = 'NI' then 46237210
      when cond.condition_source = 'OT' then 45878142
      when cond.condition_source = 'UN' then 0 --charles mapping doc ref. Unknown (concept_id = 45877986/charles mapping doc ref. Unknown (concept_id = 45877986)
      when cond.condition_source in ('RG', 'DR', 'PC') then 0 ---PC    PC=PCORnet-defined  condition  algorithm       See mapping comments,
      ELSE 32817
      END AS PROCEDURE_TYPE_CONCEPT_ID,   
proc.providerid::int as PROVIDER_ID,
0 as QUANTITY,
proc.encounterid::int as VISIT_OCCURRENCE_ID,
'PCORNET_CONDITION' AS DOMAIN_SOURCE,
'stlouis' as site
FROM stlouis_pcornet.CONDITION cond
inner join stlouis_pcornet.procedures proc on proc.encounterid = cond.encounterid and proc.patid = cond.patid
left JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on cond.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Procedure'
                                                       --and xw.target_concept_id=mp.target_concept_id
                                                       and Xw.Src_Code_Type=cond.condition_type;
commit;													   