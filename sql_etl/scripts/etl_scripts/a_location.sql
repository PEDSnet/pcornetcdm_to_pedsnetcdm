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
 WHERE enc.facilityid IS NOT NULL
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
select distinct
 	nextval('SITE_pedsnet.loc_seq') AS location_id,
    address_city as city,
    address_state as state,
	zip as zip,
	'patient history | ' || zip || ' | geocode | ' || coalesce(GEOCODE_STATE,'') || ' | ' || coalesce(GEOCODE_COUNTY,'') || ' | ' || coalesce(GEOCODE_TRACT,'') || ' | ' || coalesce(GEOCODE_GROUP,'') as location_source_value,
    42046186 as country_concept_id,
    'United States' as country_source_value
FROM 
    (select 
        addressid,
        coalesce(address_zip5,address_zip9) as zip,
        address_city,
        address_state
    from SITE_pcornet.lds_address_history
	) as lds
left join 
    SITE_pcornet.private_address_geocode pag
    on pag.addressid = lds.addressid
GROUP BY 
    zip, 
    address_city,
    address_state,
    GEOCODE_STATE,
    GEOCODE_COUNTY,
    GEOCODE_TRACT,
    GEOCODE_GROUP
;
commit;

-- census block group locations
-- begin;
-- INSERT INTO SITE_pedsnet.location(
--     location_id,
--     location_source_value,
--     census_block_group)
-- select 
--  	nextval('SITE_pedsnet.loc_seq') AS location_id,
--     'census block group | ' || GEOCODE_BLOCK as location_source_value,
--     GEOCODE_BLOCK as census_block_group
--  FROM 
--     SITE_pcornet.PRIVATE_ADDRESS_GEOCODE
--  GROUP BY 
--     GEOCODE_BLOCK;
-- commit;

 -- default location
 begin;
 INSERT INTO SITE_pedsnet.location(
    location_id)
 values(9999999);
commit;
