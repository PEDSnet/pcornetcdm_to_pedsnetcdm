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
	zip as zip,
	zip as location_source_value,
	'SITE' as site
FROM 
    (select 
        case
            when address_zip5 is not null then address_zip5
            else address_zip9
        end as zip
    from seattle_pcornet.lds_address_history
	) as lds
where zip is not null
GROUP BY zip;

 -- default location

 INSERT INTO SITE_pedsnet.location(
    location_id,
    site)
 values(9999999,'SITE');
commit;