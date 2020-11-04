begin;
INSERT INTO pcornet_pedsnet.care_site(care_site_id, care_site_name, care_site_source_value, 
location_id, place_of_service_concept_id, place_of_service_source_value, specialty_concept_id, 
specialty_source_value, domain_source, site)
SELECT distinct on (facilityid)facilityid::int  AS care_site_id,
enc.facility_type AS care_site_name,
substr(enc.facility_type, 1, 50) AS care_site_source_value,
facility_location::int AS location_id,
coalesce(place.source_concept_id::int , 44814650) AS place_of_service_concept_id,
substr(enc.facility_type, 1, 50) AS place_of_service_source_value,  -- ehr/encounter
coalesce(spec.source_concept_id::int,44814650)  as speciality_concept_id,
spec.concept_description as specialty_source_value,
'PCORNET_ENCOUNTER' AS domain_source,
'SITE' as site
FROM SITE_pcornet.encounter enc
left join SITE_pcornet.provider prov on prov.providerid = enc.providerid
JOIN cdmh_staging.p2o_facility_type_xwalk   fx ON fx.cdm_tbl = 'ENCOUNTER'
                                             AND fx.cdm_source = 'PCORnet'
                                             AND fx.src_facility_type = enc.facility_type
join pcornet_maps.pedsnet_pcornet_valueset_map spec on spec.source_concept_class='Provider Specialty'
                                             and spec.target_concept = prov.provider_specialty_primary
											 and spec.source_concept_id is not null	
left join pcornet_maps.pedsnet_pcornet_valueset_map place on place.target_concept = enc.facility_type
                                                        and place.source_concept_id is not null
														and place.value_as_concept_id is null
                                                        and place.source_concept_class='Facility type'
WHERE enc.facility_type IS NOT NULL;
commit;