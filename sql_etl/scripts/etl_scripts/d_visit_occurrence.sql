begin;

create or replace function SITE_pedsnet.is_date(s varchar) returns boolean as $$
begin
          perform s::date;
          return true;
        exception when others then
                  return false;
end;
$$ language plpgsql;

INSERT INTO SITE_pedsnet.visit_occurrence ( 
    visit_occurrence_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    care_site_id,
    discharged_to_concept_id,
    discharged_to_source_value, 
    person_id,
    preceding_visit_occurrence_id,
    provider_id, 
    visit_concept_id, 
    visit_end_date, 
    visit_end_datetime, 
    visit_source_concept_id, 
    visit_source_value, 
    visit_start_date, 
    visit_start_datetime, 
    visit_type_concept_id)
SELECT distinct
    encounterid AS visit_occurrence_id,
    vsrc.target_concept_id AS admitted_from_concept_id,
    coalesce(vsrc.SRC_TYPE_DISCRIPTION,'') || ' | ' || coalesce(enc.admitting_source,enc.raw_admitting_source) AS admitted_from_source_value,
    cs.care_site_id as care_site_id,
    disp.target_concept_id AS discharged_to_concept_id,
    coalesce(disp.SRC_DISCHARGE_STATUS_DISCRP,'') || ' | ' || coalesce(enc.discharge_status,enc.raw_discharge_status) AS discharged_to_source_value,
    person.person_id AS person_id,
    NULL AS preceding_visit_occurrence_id,
    enc.providerid AS provider_id,
    coalesce(typ.source_concept_id::int,0) AS visit_concept_id,
    case 
        when SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::date
        when SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date::date
		else '9999-12-31'::date
    end AS visit_end_date,
    case 
        when SITE_pedsnet.is_date(enc.discharge_date::varchar) then enc.discharge_date::timestamp
        when SITE_pedsnet.is_date(enc.admit_date::varchar) then enc.admit_date::timestamp
		else '9999-12-31'::timestamp
    end AS visit_end_datetime,
    0 AS visit_source_concept_id,
    enc.encounterid AS visit_source_value,
    enc.admit_date AS visit_start_date,
    (enc.admit_date)::timestamp AS visit_start_datetime,
    44818518 AS visit_type_concept_id
FROM 
    SITE_pcornet.encounter enc
inner join 
    SITE_pedsnet.person person 
    on enc.patid=person.person_source_value
left join
    SITE_pedsnet.care_site cs
    on enc.facilityid = cs.care_site_id
LEFT JOIN 
    cdmh_staging.p2o_admitting_source_xwalk vsrc 
    ON vsrc.cdm_tbl = 'ENCOUNTER'
    AND vsrc.cdm_source = 'PCORnet'
    AND vsrc.src_admitting_source_type = enc.admitting_source
LEFT JOIN 
    cdmh_staging.p2o_discharge_status_xwalk disp 
    ON disp.cdm_tbl = 'ENCOUNTER'
    AND disp.cdm_source = 'PCORnet'
    AND disp.src_discharge_status = enc.discharge_status
left join 
    pcornet_maps.pcornet_pedsnet_valueset_map typ 
    on typ.target_concept = enc.enc_type 
    and typ.source_concept_class = 'Encounter type'
    and source_concept_id not in ('2000000469','42898160')
ON CONFLICT (visit_occurrence_id) DO NOTHING;                                         
commit;                                 
