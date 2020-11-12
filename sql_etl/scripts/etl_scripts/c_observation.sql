create sequence SITE_pedsnet.obs_seq;

begin;
-- adding discharge statue AM info encounters
INSERT INTO SITE_pedsnet.observation(observation_concept_id, observation_date, observation_datetime, observation_id,
observation_source_concept_id, observation_source_value, observation_type_concept_id, person_id, provider_id, 
qualifier_concept_id, qualifier_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, 
value_as_string, visit_occurrence_id, domain_source, site)
SELECT 4021968 AS observation_concept_id,
coalesce(enc.discharge_date,enc.admit_date)AS observation_date,
coalesce(enc.discharge_date,enc.admit_date) AS observation_datetime,
nextval('pcornet_pedsnet.obs_seq')::int AS observation_id,
44814692 AS observation_source_concept_id,
'Discharge Status-AM' AS observation_source_value,
32823 AS observation_type_concept_id,
enc.patid::int AS person_id,
enc.providerid::int AS provider_id,
NULL AS qualifier_concept_id,
NULL AS qualifier_source_value,
NULL AS unit_concept_id,
NULL AS unit_source_value,
NULL AS value_as_concept_id,           
NULL AS value_as_number,
NULL AS value_as_string,
enc.encounterid::int AS visit_occurrence_id,           
'PCORNET_ENCOUNTER' domain_source,
'SITE' as site
FROM SITE_pcornet.encounter enc
WHERE enc.discharge_status = 'AM';

commit;

begin;
-- encounter dischrge info
INSERT INTO SITE_pedsnet.observation(observation_concept_id, observation_date, observation_datetime, observation_id,
observation_source_concept_id, observation_source_value, observation_type_concept_id, person_id, provider_id, 
qualifier_concept_id, qualifier_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, 
value_as_string, visit_occurrence_id, domain_source, site)
SELECT 4137274 AS observation_concept_id,
coalesce(enc.discharge_date,enc.admit_date) AS observation_date,
coalesce(enc.discharge_date,enc.admit_date) AS observation_datetime,
nextval('pcornet_pedsnet.obs_seq')::int  AS observation_id,
CASE WHEN enc.discharge_status = 'AW' THEN 306685000
     WHEN enc.discharge_status = 'HO' THEN 44814696
     WHEN enc.discharge_status = 'IP' THEN 44814698
END AS observation_source_concept_id,
CASE WHEN enc.discharge_status = 'AW' THEN 'Discharge Status-AW'
      WHEN enc.discharge_status = 'HO' THEN 'Discharge Status-HO'
     WHEN enc.discharge_status = 'IP' THEN 'Discharge Status-IP'
END AS observation_source_value,
32823 AS observation_type_concept_id,
enc.patid::int AS person_id,
enc.providerid::int AS provider_id,           
NULL AS qualifier_concept_id,
NULL AS qualifier_source_value,
NULL AS unit_concept_id,
NULL AS unit_source_value,
NULL AS value_as_concept_id,           
NULL AS value_as_number,
NULL AS value_as_string,
enc.encounterid::int AS visit_occurrence_id, 
'PCORNET_ENCOUNTER' as domain_source,
'SITE' as site
FROM SITE_pcornet.encounter enc
WHERE enc.discharge_status IN ('AW','HO','IP');

COMMIT;

begin;
-- adding encounter discharge info
INSERT INTO SITE_pedsnet.observation(observation_concept_id, observation_date, observation_datetime, observation_id,
observation_source_concept_id, observation_source_value, observation_type_concept_id, person_id, provider_id, 
qualifier_concept_id, qualifier_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, 
value_as_string, visit_occurrence_id, domain_source, site) 
SELECT 4216643 AS observation_concept_id,
coalesce(enc.discharge_date,enc.admit_date) AS observation_date,
coalesce(enc.discharge_date,enc.admit_date) AS observation_datetime,
nextval('pcornet_pedsnet.obs_seq')::int  AS observation_id,
4216643 AS observation_source_concept_id,         
'Discharge Status-EX' AS observation_source_value,
44818516 AS observation_type_concept_id,
enc.patid::int AS person_id,
enc.providerid::int AS provider_id,           
NULL AS qualifier_concept_id,          
NULL AS qualifier_source_value, 
NULL AS unit_concept_id,
NULL AS unit_source_value,
NULL AS value_as_concept_id,
NULL AS value_as_number,
NULL AS value_as_string,
enc.encounterid::int AS visit_occurrence_id,
'PCORNET_ENCOUNTER' domain_source,
'SITE' as site
FROM SITE_pcornet.encounter enc
WHERE enc.discharge_status = 'EX';
commit;

-- adding condition information
INSERT INTO SITE_pedsnet.observation(observation_concept_id, observation_date, observation_datetime, observation_id,
observation_source_concept_id, observation_source_value, observation_type_concept_id, person_id, provider_id, 
qualifier_concept_id, qualifier_source_value, unit_concept_id, unit_source_value, value_as_concept_id, value_as_number, 
value_as_string, visit_occurrence_id, domain_source, site) 
select distinct
xw.target_concept_id as OBSERVATION_CONCEPT_ID,
cond.report_date as OBSERVATION_DATE,
cond.report_date::timestamp as OBSERVATION_DATETIME,
nextval('pcornet_pedsnet.obs_seq')::int as OBSERVATION_ID,
xw.source_code_concept_id as OBSERVATION_SOURCE_CONCEPT_ID, 
cond.raw_condition AS OBSERVATION_SOURCE_VALUE,
case when cond.condition_source = 'HC' then 38000245
     when cond.condition_source ='PR' then 45905770
     when cond.condition_source = 'NI' then 46237210
     when cond.condition_source = 'OT' then 45878142
     when cond.condition_source = 'UN' then 45877986  
     when cond.condition_source in ('RG', 'DR', 'PC') then 0 
     else 32817
     end as OBSERVATION_TYPE_CONCEPT_ID, 
cond.patid::int as PERSON_ID,
enc.providerid::int as PROVIDER_ID,            
0 as QUALIFIER_CONCEPT_ID,
null as QUALIFIER_SOURCE_VALUE,
0 as UNIT_CONCEPT_ID,
Null as UNIT_SOURCE_VALUE,
xw.source_code_concept_id as VALUE_AS_CONCEPT_ID ,
0 VALUE_AS_NUMBER,
0 as VALUE_AS_STRING,
enc.encounterid::int as VISIT_OCCURRENCE_ID,            
'PCORNET_CONDITION' DOMAIN_SOURCE,
'SITE' as site
FROM SITE_pcornet.CONDITION cond
left join stlouis_pcornet.encounter enc on enc.encounterid = cond.encounterid
left JOIN CDMH_STAGING.p2o_code_xwalk_standard xw on cond.condition = xw.src_code and xw.CDM_TBL='CONDITION' AND xw.target_domain_id = 'Observation'
                                        and Xw.Src_Code_Type=cond.condition_type;