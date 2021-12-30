begin;
INSERT INTO SITE_pedsnet.location(
    location_id,
    zip,
    site)
select 
	distinct
		dense_rank() over (order by facility_location)::bigint  AS location_id,
		facility_location as zip,
		facility_location as location_source_value,
		'SITE' as site
 FROM SITE_pcornet.encounter enc
 WHERE enc.facility_type IS NOT NULL;

 -- default location

 INSERT INTO SITE_pedsnet.location(
    location_id,
    site)
 values(9999999,'SITE')
commit;