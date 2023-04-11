create sequence if not exists SITE_pedsnet.obs_seq;

create or replace function SITE_pedsnet.is_date(s varchar) returns boolean as $$
begin
          perform s::date;
          return true;
        exception when others then
                  return false;
end;
$$ language plpgsql;

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
     value_source_value, 
     visit_occurrence_id)
SELECT distinct
     44813951 AS observation_concept_id,
     case 
          when enc.discharge_date is not null and  SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::date
          when enc.admit_date is not null and SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date:: date
          else '0001-01-01'::date
     end AS observation_date,
     case 
          when SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::timestamp
          when enc.admit_date is not null and SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date:: timestamp
          else '0001-01-01'::timestamp
     end AS observation_datetime,
     nextval('SITE_pedsnet.obs_seq') AS observation_id,
     0 AS observation_source_concept_id,
     'Discharge Status' AS observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid AS person_id,
     enc.providerid AS provider_id,
     NULL::integer AS qualifier_concept_id,
     NULL AS qualifier_source_value,
     NULL::integer AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when enc.discharge_disposition='A' then 4161979 -- Alive
          when enc.discharge_disposition='E' then 4216643 -- Expired
          when enc.discharge_disposition='NI' then 44814650 -- No Information
          when enc.discharge_disposition='OT' then 44814649 -- Other
          when enc.discharge_disposition='UN' then 44814653 -- Unknown
     end as value_as_concept_id,           
     NULL::numeric AS value_as_number,
     NULL AS value_as_string,
     enc.discharge_disposition as value_source_value,
     enc.encounterid AS visit_occurrence_id
FROM 
     SITE_pcornet.encounter enc
WHERE 
     enc.discharge_disposition is not null;
commit;

begin;
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
     value_source_value, 
     visit_occurrence_id)
SELECT distinct
     3040464 AS observation_concept_id,
     case 
          when enc.discharge_date is not null and SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::date
          when enc.admit_date is not null and SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date::date
          else '0001-01-01'::date
     end AS observation_date,
     case 
          when enc.discharge_date is not null and SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::timestamp
          when enc.admit_date is not null and SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date::timestamp
          else '0001-01-01'::timestamp
     end AS observation_datetime,
     nextval('SITE_pedsnet.obs_seq') AS observation_id,
     0 AS observation_source_concept_id,
     'DRG|'||enc.DRG AS observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid AS person_id,
     enc.providerid AS provider_id,
     4269228 AS qualifier_concept_id,
     'Primary' AS qualifier_source_value, -- Only primary DRG recorded in PCORnet
     NULL::integer AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when 
		     case 
                    when SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::date < '01-OCT-2007'
		          when SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date::date < '01-OCT-2007'
		     end
          then drg.concept_id 
          else msdrg.concept_id
     end as value_as_concept_id,           
     NULL::numeric AS value_as_number,
     NULL AS value_as_string,
     case 
          when
	          case 
                    when SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::date < '01-OCT-2007'
                    when SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date::date < '01-OCT-2007'
               end
          then drg.concept_id 
          else msdrg.concept_id
     end::varchar as value_source_value,
     enc.encounterid AS visit_occurrence_id
FROM 
     SITE_pcornet.encounter enc
left join 
     vocabulary.concept drg 
     on enc.drg=drg.concept_code 
     and drg.concept_class_id = 'DRG' 
     and valid_end_date = '30-SEP-2007' 
     and invalid_reason = 'D' 
left join 
     vocabulary.concept msdrg 
     on enc.drg=msdrg.concept_code 
     and msdrg.concept_class_id = 'MS-DRG' 
     and msdrg.invalid_reason is null 
WHERE 
     enc.DRG is not null;
commit;

begin;
--add vital smoking/tobacco information
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
     value_source_value,
     visit_occurrence_id)
SELECT distinct
     4005823 AS observation_concept_id,
     case 
          when vt.measure_date is not null then vt.measure_date::date
          else '0001-01-01'::date
     end AS observation_date,
     case 
          when vt.measure_time is not null and vt.measure_date is not null then (vt.measure_date || ' '|| vt.measure_time)::timestamp
          else '0001-01-01'::timestamp 
     end AS observation_datetime,
     nextval('SITE_pedsnet.obs_seq') AS observation_id,
     0 AS observation_source_concept_id,
     'Tobacco|'||vt.tobacco||'|'||coalesce(vt.raw_tobacco,' ') as observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid AS person_id,
     enc.providerid AS provider_id,
     0 AS qualifier_concept_id,
     null AS qualifier_source_value,
     NULL::integer AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when tobacco='01' then 4005823 --Current user
          when tobacco='02' then 45765920 --Never
          when tobacco='03' then 45765917 --Quit/former user
          when tobacco='04' then 4030580 --Passive or environmental exposure
          when tobacco='06' then 2000000040 --Not asked
          when tobacco='NI' then 44814650 --No information
          when tobacco='UN' then 44814649 --Unknown
          when tobacco='OT' then 44814653 --Other
     end as value_as_concept_id,           
     NULL::numeric AS value_as_number,
     NULL AS value_as_string,
     tobacco as value_source_value,
     enc.encounterid AS visit_occurrence_id
FROM 
     SITE_pcornet.vital vt
left join 
     SITE_pcornet.encounter enc 
     on vt.encounterid = enc.encounterid
WHERE 
     vt.tobacco is not null and
     enc.patid is not null;
commit;

begin;
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
     value_source_value,
     visit_occurrence_id)
SELECT distinct
     4219336 AS observation_concept_id,
     case 
          when vt.measure_date is not null then vt.measure_date::date 
          else '0001-01-01':: date 
     end AS observation_date,
     case 
          when vt.measure_time is not null and vt.measure_date is not null then (vt.measure_date|| ' '|| vt.measure_time)::timestamp
          else '0001-01-01'::timestamp
     end AS observation_datetime,
     nextval('SITE_pedsnet.obs_seq') AS observation_id,
     0 AS observation_source_concept_id,
     'Tobacco Type|'||vt.tobacco_type||'|'||coalesce(vt.raw_tobacco_type,' ') as observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid AS person_id,
     enc.providerid AS provider_id,
     0 AS qualifier_concept_id,
     null AS qualifier_source_value,
     NULL::integer AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when tobacco_type='01' then 42530793 --Smoked Tobaccbo only
          when tobacco_type='02' then 42531042 --Non-Smoked Tobaccbo only
          when tobacco_type='03' then 45765917 --Use of both smoked and non-smoked tobacco
          when tobacco_type='04' then 45878582--None
          when tobacco_type='05' then 42531020 --Use of smoked tobacco but no information about non-smoked tobacco use
          when tobacco_type='NI' then 44814650 --No information
          when tobacco_type='UN' then 44814649 --Unknown
          when tobacco_type='OT' then 44814653 --Other
         end as value_as_concept_id,           
     NULL::numeric AS value_as_number,
     NULL AS value_as_string,
     tobacco_type as value_source_value,
     enc.encounterid AS visit_occurrence_id
FROM 
     SITE_pcornet.vital vt
left join 
     SITE_pcornet.encounter enc 
     on vt.encounterid = enc.encounterid
WHERE 
     vt.tobacco_type is not null and
     enc.patid is not null;
commit;

begin;
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
     value_source_value,
     visit_occurrence_id)
SELECT distinct
     4275495 AS observation_concept_id,
     case 
          when vt.measure_date is not null then vt.measure_date::date
          else '0001-01-01'::date	
	end AS observation_date,
     case 
          when vt.measure_time is not null and vt.measure_date is not null then (vt.measure_date || ' '|| vt.measure_time)::timestamp
          else '0001-01-01'::timestamp 
     end AS observation_datetime,
     nextval('SITE_pedsnet.obs_seq') AS observation_id,
     0 AS observation_source_concept_id,
     'Smoking|'||vt.smoking||'|'||coalesce(vt.raw_smoking,' ') AS observation_source_value,
     38000280 AS observation_type_concept_id,
     enc.patid AS person_id,
     enc.providerid AS provider_id,
     0 AS qualifier_concept_id,
     null AS qualifier_source_value,
     NULL::integer AS unit_concept_id,
     NULL AS unit_source_value,
     case 
          when smoking='01' then 42709996 --Current everyday smoker
          when smoking='02' then 37395605 --current some day smoker
          when smoking='03' then 4310250 --Ex-smoker
          when smoking='04' then 4144272--Never Smoker
          when smoking='05' then 4298794 --Smoker, current status unknown
          when smoking='06' then 4141786 -- Unknown if ever smoked
          when smoking='NI' then 44814650 --No information
          when smoking='UN' then 44814649 --Unknown
          when smoking='OT' then 44814653 --Other
         end as value_as_concept_id,           
     NULL::numeric AS value_as_number,
     NULL AS value_as_string,
     smoking as value_source_value,
     enc.encounterid AS visit_occurrence_id
FROM 
     SITE_pcornet.vital vt
left join 
     SITE_pcornet.encounter enc 
     on vt.encounterid = enc.encounterid
WHERE 
     vt.smoking is not null
     and enc.patid is not null;
commit;

-- add sexual_orientation
-- AS=Asexual 
-- BI=Bisexual 
-- GA=Gay 
-- LE=Lesbian 
-- QU=Queer 
-- QS=Questioning 
-- ST=Straight 
-- SE=Something else 
-- MU=Multiple sexual orientations 
-- DC=Decline to answer
-- NI=No information 
-- UN=Unknown
-- OT=Other

--add gender identity
