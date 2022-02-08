create sequence if not exists SITE_pedsnet.loc_seq;

begin;

INSERT INTO SITE_pedsnet.location(
    location_id,
    zip,
    location_source_value,
    site)
select 
		nextval('SITE_pedsnet.loc_seq')::bigint AS location_id,
		facility_location as zip,
		facilityid as location_source_value,
		'SITE' as site
 FROM SITE_pcornet.encounter enc
 WHERE enc.facility_type IS NOT NULL
 GROUP BY facility_location, facilityid;

INSERT INTO SITE_pedsnet.location(
    location_id,
    zip,
    location_source_value,
    site)
select 
	nextval('SITE_pedsnet.loc_seq')::bigint AS location_id,
	address_zip5 as zip,
	address_zip9 as location_source_value,
	'SITE' as site
 FROM SITE_pcornet.lds_address_history lds
 GROUP BY address_zip5, address_zip9;

 -- default location

 INSERT INTO SITE_pedsnet.location(
    location_id,
    site)
 values(9999999,'SITE');
commit;