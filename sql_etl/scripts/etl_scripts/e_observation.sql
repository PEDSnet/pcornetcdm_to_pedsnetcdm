create sequence SITE_pedsnet.obs_seq;

begin;
-- adding PEDSnet discharge status (PCORnet discharge disposition) 
INSERT INTO SITE_pedsnet.observation(
     observation_concept_id,
     observation_date, 
     observation_datetime, 
     observation_id,
     observation_source_concept_id, 
     observation_source_value, 
     observation_type_concept_id, 
     person_id, 
     provider_id, 
     qualifier_concept_id, 
     qualifier_source_value, 
     unit_concept_id, 
     unit_source_value, 
     value_as_concept_id, 
     value_as_number, 
     value_as_string, 
     visit_occurrence_id, 
     site)
SELECT 
     44813951 AS observation_concept_id,
     coalesce(enc.discharge_date,enc.admit_date)AS observation_date,
     coalesce(enc.discharge_date,enc.admit_date) AS observation_datetime,
     nextval('pcornet_pedsnet.obs_seq')::bigint AS observation_id,
     0 AS observation_source_concept_id,
     'Discharge Status' AS observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid::bigint AS person_id,
     enc.providerid::bigint AS provider_id,
     NULL AS qualifier_concept_id,
     NULL AS qualifier_source_value,
     NULL AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when enc.discharge_disposition='A' then 4161979 -- Alive
          when enc.discharge_disposition='E' then 4216643 -- Expired
          when enc.discharge_disposition='NI' then 444814650 -- No Information
          when enc.discharge_disposition='OT' then 44814649 -- Other
          when enc.discharge_disposition='UN' then 44814653 -- Unknown
     end as value_as_concept_id,           
     NULL AS value_as_number,
     NULL AS value_as_string,
     enc.encounterid::bigint AS visit_occurrence_id,           
     'SITE' as site
FROM SITE_pcornet.encounter enc
WHERE enc.discharge_disposition is not null;

commit;


-- adding DRG
INSERT INTO SITE_pedsnet.observation(
     observation_concept_id,
     observation_date, 
     observation_datetime, 
     observation_id,
     observation_source_concept_id, 
     observation_source_value, 
     observation_type_concept_id, 
     person_id, 
     provider_id, 
     qualifier_concept_id, 
     qualifier_source_value, 
     unit_concept_id, 
     unit_source_value, 
     value_as_concept_id, 
     value_as_number, 
     value_as_string, 
     visit_occurrence_id, 
     site)
SELECT 
     3040464 AS observation_concept_id,
     coalesce(enc.discharge_date,enc.admit_date)AS observation_date,
     coalesce(enc.discharge_date,enc.admit_date) AS observation_datetime,
     nextval('pcornet_pedsnet.obs_seq')::bigint AS observation_id,
     0 AS observation_source_concept_id,
     'DRG|'||enc.DRG AS observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid::bigint AS person_id,
     enc.providerid::bigint AS provider_id,
     4269228 AS qualifier_concept_id,
     'Primary' AS qualifier_source_value, -- Only primary DRG recorded in PCORnet
     NULL AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when coalesce(enc.discharge_date,enc.admit_date)::date < '01-OCT-2007'
          then 
               drg.concept_id 
               else msdrg.concept_id
         end as value_as_concept_id,           
     NULL AS value_as_number,
     NULL AS value_as_string,
     enc.encounterid::bigint AS visit_occurrence_id,           
     'SITE' as site
FROM SITE_pcornet.encounter enc
left join vocabulary.concept drg on enc.drg=drg.concept_code and drg.concept_class_id = 'DRG' and valid_end_date = '30-SEP-2007' and invalid_reason = 'D' 
left join vocabulary.concept msdrg on enc.drg=msdrg.concept_code and msdrg.concept_class_id = 'MS-DRG' and msdrg.invalid_reason is null 
WHERE enc.DRG is not null;

commit;

--add vital smoking/tobacco information



