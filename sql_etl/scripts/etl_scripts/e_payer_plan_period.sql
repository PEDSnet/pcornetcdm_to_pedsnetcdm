INSERT INTO CDMH_STAGING.ST_OMOP53_PAYER_PLAN_PERIOD
(
DATA_PARTNER_ID,
MANIFEST_ID,
PAYER_PLAN_PERIOD_ID,
PERSON_ID,
PAYER_PLAN_PERIOD_START_DATE,
PAYER_PLAN_PERIOD_END_DATE,
PAYER_CONCEPT_ID,
PAYER_SOURCE_VALUE,
PAYER_SOURCE_CONCEPT_ID,
PLAN_CONCEPT_ID,
PLAN_SOURCE_VALUE,
PLAN_SOURCE_CONCEPT_ID,
SPONSOR_CONCEPT_ID,
SPONSOR_SOURCE_VALUE,
SPONSOR_SOURCE_CONCEPT_ID,
FAMILY_SOURCE_VALUE,
STOP_REASON_CONCEPT_ID,
STOP_REASON_SOURCE_VALUE,
STOP_REASON_SOURCE_CONCEPT_ID,
DOMAIN_SOURCE
)
SELECT 
DATA_PARTNER_ID,
MANIFEST_ID,
PAYER_PLAN_PERIOD_ID,
PERSON_ID,
PAYER_PLAN_PERIOD_START_DATE,
PAYER_PLAN_PERIOD_END_DATE,
PAYER_CONCEPT_ID,
PAYER_SOURCE_VALUE,
PAYER_SOURCE_CONCEPT_ID,
PLAN_CONCEPT_ID,
PLAN_SOURCE_VALUE,
PLAN_SOURCE_CONCEPT_ID,
SPONSOR_CONCEPT_ID,
SPONSOR_SOURCE_VALUE,
SPONSOR_SOURCE_CONCEPT_ID,
FAMILY_SOURCE_VALUE,
STOP_REASON_CONCEPT_ID,
STOP_REASON_SOURCE_VALUE,
STOP_REASON_SOURCE_CONCEPT_ID,
DOMAIN_SOURCE
FROM (

SELECT /*+ use_hash */
    datapartnerid AS DATA_PARTNER_ID,
    manifestid AS MANIFEST_ID,
    mp.N3CDS_DOMAIN_MAP_ID as payer_plan_period_id,
    p.N3CDS_DOMAIN_MAP_ID as person_id,
    e.ADMIT_DATE as payer_plan_period_start_date,
    case when e.DISCHARGE_DATE is not null then e.DISCHARGE_DATE
        else e.ADMIT_DATE
        end as payer_plan_period_end_date,
    xw.target_concept_id as payer_concept_id, --get the list of OMOP concept_ids
    'PAYER_TYPE_PRIMARY ' || e.PAYER_TYPE_PRIMARY as payer_source_value, --this one can stay as the PCORnet source value
    null as payer_source_concept_id,
    null as plan_concept_id,
    null as plan_source_value,
    null as plan_source_concept_id,
    null as sponsor_concept_id,
    null as sponsor_source_value,
    null as sponsor_source_concept_id,
    null as family_source_value,
    null as stop_reason_concept_id,
    null as stop_reason_source_value,
    null as stop_reason_source_concept_id,
    'PCORNET_ENCOUNTER' AS DOMAIN_SOURCE,
    row_number() over (partition by e.patid,to_char(admit_date,'YYYY-MM-DD'),nvl(to_char(discharge_date,'YYYY-MM-DD'),to_char(admit_date,'YYYY-MM-DD')),e.PAYER_TYPE_PRIMARY 
    order by admit_date desc,nvl(admit_date,discharge_date) desc, encounterid
    )
    as rnk
FROM
    NATIVE_PCORNET51_CDM.ENCOUNTER e
    JOIN CDMH_STAGING.PERSON_CLEAN pc ON e.PATID=pc.PERSON_ID 
                                AND pc.DATA_PARTNER_ID=datapartnerid 
    JOIN CDMH_STAGING.P2O_TERM_XWALK xw on xw.SRC_CODE=e.payer_type_primary 
                                AND xw.cdm_tbl_column_name= 'PAYER_TYPE_PRIMARY'  
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP p ON p.SOURCE_ID=e.PATID AND p.DATA_PARTNER_ID=datapartnerid AND p.domain_name='PERSON'                            
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on e.ENCOUNTERID=Mp.Source_Id
                                AND mp.DOMAIN_NAME='ENCOUNTER' 
                                AND Mp.Target_Domain_Id = 'PAYER_TYPE_PRIMARY' 
                                AND mp.DATA_PARTNER_ID=datapartnerid   
                                AND mp.TARGET_CONCEPT_ID=xw.TARGET_CONCEPT_ID
                                
WHERE
    payer_type_primary is not null 

UNION ALL
 
--secondary payors
SELECT /*+ use_hash */
    datapartnerid AS DATA_PARTNER_ID,
    manifestid AS MANIFEST_ID,
    mp.N3CDS_DOMAIN_MAP_ID as payer_plan_period_id,
    p.N3CDS_DOMAIN_MAP_ID as person_id,
    e.ADMIT_DATE as payer_plan_period_start_date,
    case when e.DISCHARGE_DATE is not null then e.DISCHARGE_DATE
        else e.ADMIT_DATE
        end as payer_plan_period_end_date,
    xw.target_concept_id as payer_concept_id, --get the list of the OMOP concept_ids
    'PAYER_TYPE_SECONDARY ' || e.PAYER_TYPE_SECONDARY as payer_source_value, --this one can stay as the PCORnet source value
    null as payer_source_concept_id,
    null as plan_concept_id,
    null as plan_source_value,
    null as plan_source_concept_id,
    null as sponsor_concept_id,
    null as sponsor_source_value,
    null as sponsor_source_concept_id,
    null as family_source_value,
    null as stop_reason_concept_id,
    null as stop_reason_source_value,
    null as stop_reason_source_concept_id,
    'PCORNET_ENCOUNTER' AS DOMAIN_SOURCE,
    row_number() over (partition by e.patid,to_char(admit_date,'YYYY-MM-DD'),nvl(to_char(discharge_date,'YYYY-MM-DD'),to_char(admit_date,'YYYY-MM-DD')),e.PAYER_TYPE_SECONDARY 
    order by admit_date desc,nvl(admit_date,discharge_date) desc, encounterid
    )
    as rnk
FROM
    NATIVE_PCORNET51_CDM.ENCOUNTER e
    JOIN CDMH_STAGING.PERSON_CLEAN pc ON e.PATID=pc.PERSON_ID 
                                AND pc.DATA_PARTNER_ID=datapartnerid 
    JOIN CDMH_STAGING.P2O_TERM_XWALK xw on xw.SRC_CODE=e.payer_type_secondary 
                                AND xw.cdm_tbl_column_name= 'PAYER_TYPE_SECONDARY'  
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP p ON p.SOURCE_ID=e.PATID AND p.DATA_PARTNER_ID=datapartnerid AND p.domain_name='PERSON'                            
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on e.ENCOUNTERID=Mp.Source_Id 
                                AND mp.DOMAIN_NAME='ENCOUNTER' 
                                AND Mp.Target_Domain_Id = 'PAYER_TYPE_SECONDARY' 
                                AND mp.DATA_PARTNER_ID=datapartnerid   
                                AND mp.TARGET_CONCEPT_ID=xw.TARGET_CONCEPT_ID
WHERE
    payer_type_secondary is not null
   ) Payer 
   where rnk=1
    ;
    payerPlancnt :=sql%rowcount;
    COMMIT;
