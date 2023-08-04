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
    specialty_source_value)
SELECT distinct 
    prov.providerid AS provider_id,
    prov.providerid || ' provider' as provider_source_value,
    coalesce(
        case
            when gender_map.source_concept_id = 'NULL' then 44814650    
            when gender_map.source_concept_id::int = 0 then 44814650
            else gender_map.source_concept_id::int
        end,   
        44814650) as gender_concept_id,
    44814650 as gender_source_concept_id,
    prov.provider_sex as gender_source_value,
    prov.provider_npi as npi,
    9999999 AS care_site_id, -- default to a default care_site id for now for not null requirement
    coalesce(
        case 
            when specialty_map.source_concept_id = 'NULL' or specialty_map.source_concept_id is null then 38004477
            else specialty_map.source_concept_id::int
        end,
        38004477) as speciality_concept_id,
    coalesce(prov.provider_specialty_primary,'')||'|'||coalesce(specialty_map.concept_description,'') as specialty_source_value
FROM 
    SITE_pcornet.provider prov
left join pcornet_maps.pcornet_pedsnet_valueset_map gender_map 
    on gender_map.source_concept_class='Gender' 
    and prov.provider_sex=gender_map.target_concept
left join pcornet_maps.pcornet_pedsnet_valueset_map specialty_map
    on specialty_map.source_concept_class = 'Provider Specialty' 
    and prov.provider_specialty_primary=specialty_map.target_concept
ON CONFLICT (provider_id) DO NOTHING;
commit;

-- encounter
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    providerid AS provider_id,
    providerid || ' encounter' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.encounter
where
    providerid not in (select provider_id from SITE_pedsnet.provider);
commit;

-- diagnosis
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    providerid AS provider_id,
    providerid || ' diagnosis' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.diagnosis
where
    providerid not in (select provider_id from SITE_pedsnet.provider);
commit;

-- procedures
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    providerid AS provider_id,
    providerid || ' procedures' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.procedures
where
    providerid not in (select provider_id from SITE_pedsnet.provider);
commit;

-- prescribing
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    RX_PROVIDERID AS provider_id,
    RX_PROVIDERID || ' prescribing' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.prescribing
where
    RX_PROVIDERID not in (select provider_id from SITE_pedsnet.provider);
commit;

-- med_admin
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    MEDADMIN_PROVIDERID AS provider_id,
    MEDADMIN_PROVIDERID || ' med_admin' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.med_admin
where
    MEDADMIN_PROVIDERID not in (select provider_id from SITE_pedsnet.provider);
commit;

-- obs_clin
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    OBSCLIN_PROVIDERID AS provider_id,
    OBSCLIN_PROVIDERID || ' obs_clin' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.obs_clin
where
    OBSCLIN_PROVIDERID not in (select provider_id from SITE_pedsnet.provider);
commit;

-- obs_gen
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    OBSGEN_PROVIDERID AS provider_id,
    OBSGEN_PROVIDERID || ' obs_gen' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.obs_gen
where
    OBSGEN_PROVIDERID not in (select provider_id from SITE_pedsnet.provider);
commit;

-- immunization
begin;
INSERT INTO SITE_pedsnet.provider(
    provider_id,
    provider_source_value,
    care_site_id,
    specialty_concept_id)
select distinct
    vx_providerid AS provider_id,
    vx_providerid || ' immunization' as provider_source_value,
    9999999 AS care_site_id,
    38004477 as specialty_concept_id
from
    SITE_pcornet.immunization
where
    vx_providerid not in (select provider_id from SITE_pedsnet.provider);
commit;