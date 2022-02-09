begin;
INSERT INTO SITE_pedsnet.care_site(
    care_site_id,
    care_site_name,
    care_site_source_value,
    location_id,
    place_of_service_concept_id,
    place_of_service_source_value,
    specialty_concept_id, 
    specialty_source_value,
    site)
SELECT 
    distinct on (facilityid)facilityid::bigint  AS care_site_id,
    enc.facility_type AS care_site_name,
    substr(enc.facility_type, 1, 50) AS care_site_source_value,
    loc.location_id location_id,
    coalesce(place.source_concept_id::int , 44814650) AS place_of_service_concept_id,
    substr(enc.facility_type, 1, 50) AS place_of_service_source_value,  -- ehr/encounter
    coalesce(facility_spec.value_as_concept_id::int,prov_spec.source_concept_id::int,44814650)  as speciality_concept_id,
    coalesce(facility_spec.target_concept,prov_spec.concept_description,'') as specialty_source_value,
    'SITE' as site
FROM SITE_pcornet.encounter enc
left join SITE_pcornet.provider prov on prov.providerid = enc.providerid
LEFT JOIN cdmh_staging.visit_xwalk vx ON vx.cdm_tbl = 'ENCOUNTER'
                                      AND vx.cdm_name = 'PCORnet'
                                      AND vx.src_visit_type = coalesce(TRIM(enc.enc_type), 'UN')
left join pcornet_maps.pedsnet_pcornet_valueset_map prov_spec on prov_spec.source_concept_class='Provider Specialty'
                                          and prov_spec.target_concept = prov.provider_specialty_primary
											          and prov_spec.source_concept_id is not null	
left join pcornet_maps.pedsnet_pcornet_valueset_map facility_spec on prov_spec.source_concept_class='Facility type'
                                             and vx.src_visit_type=facility_spec.source_concept_id
                                             and enc.facility_type=facility_spec.target_concept
                                             and prov_spec.target_concept = prov.provider_specialty_primary
left join pcornet_maps.pedsnet_pcornet_valueset_map place on place.target_concept = enc.facility_type
                                                        and place.source_concept_id is not null
														 and place.value_as_concept_id is null
                                                        and place.source_concept_class='Facility type'
left join SITE_pedsnet.location loc on enc.facility_location=loc.zip and enc.facilityid = loc.location_source_value
WHERE enc.facility_type IS NOT NULL
;

commit;

-- default care site

INSERT INTO SITE_pedsnet.care_site(
    care_site_id,
    location_id,
    place_of_service_concept_id,
    specialty_concept_id, 
    site)
values(9999999,9999999,44814650,44814650,'SITE');
commit;