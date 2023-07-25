
CREATE OR REPLACE FUNCTION SITE_pedsnet.isnumeric(character varying) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
        x = $1::NUMERIC;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
                RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

begin;
INSERT INTO SITE_pedsnet.care_site(
    care_site_id,
    care_site_name,
    care_site_source_value,
    location_id,
    place_of_service_concept_id,
    place_of_service_source_value,
    specialty_concept_id, 
    specialty_source_value)
SELECT 
    distinct on (facilityid)facilityid AS care_site_id,
    enc.facility_type AS care_site_name,
    coalesce(
        case when enc.facility_type is not null then substr(enc.facility_type, 1, 50) else null end,
        enc.RAW_FACILITY_TYPE,
        enc.facilityid
    ) AS care_site_source_value,
    loc.location_id location_id,
    case 
        when(SITE_pedsnet.isnumeric(place.source_concept_id::varchar)) then place.source_concept_id::int 
        else 44814650 
    end AS place_of_service_concept_id,
    substr(enc.facility_type, 1, 50) AS place_of_service_source_value,
    case 
        when(SITE_pedsnet.isnumeric(facility_spec.value_as_concept_id::varchar)) then facility_spec.value_as_concept_id::int 
        when (SITE_pedsnet.isnumeric(prov_spec.source_concept_id::varchar)) then prov_spec.source_concept_id::int 
        else 44814650 
    end as speciality_concept_id,
    coalesce(
        facility_spec.target_concept,
        prov_spec.concept_description,
        enc.facility_type
    ) as specialty_source_value
FROM 
    SITE_pcornet.encounter enc
left join 
    SITE_pcornet.provider prov 
    on prov.providerid = enc.providerid
LEFT JOIN 
    cdmh_staging.visit_xwalk vx 
    ON vx.cdm_tbl = 'ENCOUNTER'
    AND vx.cdm_name = 'PCORnet'
    AND vx.src_visit_type = coalesce(TRIM(enc.enc_type), 'UN')
left join 
    pcornet_maps.pcornet_pedsnet_valueset_map prov_spec 
    on prov_spec.source_concept_class='Provider Specialty'
    and prov_spec.target_concept = prov.provider_specialty_primary
    and prov_spec.source_concept_id is not null 
 left join 
     pcornet_maps.pcornet_pedsnet_valueset_map facility_spec 
     on prov_spec.source_concept_class='Facility type'
     and vx.src_visit_type=facility_spec.source_concept_id
     and enc.facility_type=facility_spec.target_concept
     and prov_spec.target_concept = prov.provider_specialty_primary
left join 
    pcornet_maps.pcornet_pedsnet_valueset_map place 
    on place.target_concept = enc.facility_type
    and place.source_concept_id is not null
    and place.value_as_concept_id is null
    and place.source_concept_class='Facility type'
left join 
    SITE_pedsnet.location loc 
    on enc.facility_location=loc.zip
WHERE 
    enc.facilityid is not null
ON CONFLICT (care_site_id) DO NOTHING;

commit;

begin;
-- default care site

INSERT INTO SITE_pedsnet.care_site(
    care_site_id,
    care_site_source_value,
    location_id,
    place_of_service_concept_id,
    specialty_concept_id)
values(9999999,'9999999',9999999,44814650,44814650);
commit;
