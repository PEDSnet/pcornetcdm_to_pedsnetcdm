begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    gender_concept_id,
    gender_source_concept_id,
    gender_source_value,
    npi,
    care_site_id,
    specialty_concept_id,
    specialty_source_value,
    site)
SELECT 
    distinct on (providerid)providerid::bigint  AS provider_id,
    prov.providerid as provider_source_value,
    coalesce(if(gender_map.source_concept_id::int = 0,44814650,gender_map.source_concept_id::int),44814650) as gender_concept_id,
    44814650 as gender_source_concept_id,
    prov.provider_sex as gender_source_value,
    prov.provider_npi as npi,
    9999999::bigint AS care_site_id, -- default to a default care_site id for now for not null requirement
    coalesce(specialty_map.source_concept_id::int,44814650)  as speciality_concept_id,
    coalesce(prov.provider_specialty_primary,'')||'|'||coalesce(specialty_map.concept_description,'') as specialty_source_value,
    'SITE' as site
FROM SITE_pcornet.provider prov
left join pcornet_maps.pedsnet_pcornet_valueset_map gender_map 
    on gender_map.source_concept_class='Gender' 
    and prov.provider_sex=gender_map.target_concept
left join pcornet_maps.pedsnet_pcornet_valueset_map specialty_map
    on specialty_map.source_concept_class = 'Provider Specialty' 
    and prov.provider_specialty_primary=specialty_map.target_concept;
commit;