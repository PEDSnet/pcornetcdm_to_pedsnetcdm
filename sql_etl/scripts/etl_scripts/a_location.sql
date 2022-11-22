create sequence if not exists SITE_pedsnet.loc_seq;

-- care site locations
begin;
INSERT INTO SITE_pedsnet.location(
    location_id,
    zip,
    location_source_value,
    country_concept_id,
    country_source_value)
select 
	nextval('SITE_pedsnet.loc_seq') AS location_id,
	facility_location as zip,
	'encounter | ' || facility_location  as location_source_value,
    42046186 as country_concept_id,
    'United States' as country_source_value
 FROM SITE_pcornet.encounter enc
 WHERE enc.facility_type IS NOT NULL
 GROUP BY facility_location;
commit;

-- address history locations
begin;
INSERT INTO SITE_pedsnet.location(
    location_id,
    city,
    state,
    zip,
    location_source_value,
    country_concept_id,
    country_source_value)
select 
 	nextval('SITE_pedsnet.loc_seq') AS location_id,
    address_city as city,
    address_state as state,
	zip as zip,
	'patient history | ' || zip as location_source_value,
    42046186 as country_concept_id,
    'United States' as country_source_value
FROM 
    (select 
        case
            when address_zip5 is not null then address_zip5
            else address_zip9
        end as zip,
        address_city,
        address_state
    from SITE_pcornet.lds_address_history
	) as lds
where zip is not null
GROUP BY zip, address_city, address_state;
commit;

-- census block group locations
begin;
INSERT INTO SITE_pedsnet.location(
    location_id,
    location_source_value,
    census_block_group)
select 
    nextval('SITE_pedsnet.loc_seq') AS location_id,
    'census block group | ' || GEOCODE_BLOCK as location_source_value,
    CASE
        when GEOCODE_BLOCK is not null then GEOCODE_BLOCK 
        when geocode_group is not null then geocode_group
    END as census_block_group
 FROM 
    SITE_pcornet.PRIVATE_ADDRESS_GEOCODE
where
    GEOCODE_BLOCK is not null
    or geocode_group is not null
 GROUP BY 
    GEOCODE_BLOCK,
    geocode_group;
commit;

 -- default location
 begin;
 INSERT INTO SITE_pedsnet.location(
    location_id)
 values(9999999);
commit;
