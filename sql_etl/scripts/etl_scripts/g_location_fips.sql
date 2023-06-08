create sequence if not exists SITE_pedsnet.geocode_seq;

begin;
insert into SITE_pedsnet.location_fips(
    geocode_id BIGINT NOT NULL AUTO_INCREMENT, 
    location_id BIGINT NOT NULL, 
    geocode_state VARCHAR(2) NOT NULL,
    geocode_county VARCHAR(3) NOT NULL, 
    geocode_tract VARCHAR(6) NOT NULL, 
    geocode_group VARCHAR(1) NOT NULL, 
	geocode_year INTEGER NOT NULL, 
	geocode_shapefile VARCHAR(256)
)
select 
    nextval('SITE_pedsnet.geocode_seq') as geocode_id,
    loc.location_id as location_id,
    case
        when SITE_pedsnet.isnumeric(GEOCODE_STATE::varchar) and length(GEOCODE_STATE) = 2 then GEOCODE_STATE
        else substring(GEOCODE_GROUP,1,2)
    end as geocode_state,
    case 
        when SITE_pedsnet.isnumeric(GEOCODE_COUNTY::varchar) and length(GEOCODE_COUNTY) = 3 then GEOCODE_COUNTY
        else substring(GEOCODE_GROUP,3,3)
    end as geocode_county,
    case 
        when SITE_pedsnet.isnumeric(GEOCODE_TRACT::varchar) and length(GEOCODE_TRACT) = 6 then GEOCODE_TRACT
        else substring(GEOCODE_GROUP,6,6)
    end as geocode_tract,
    case 
        when SITE_pedsnet.isnumeric(GEOCODE_GROUP::varchar) and length(GEOCODE_GROUP) = 1 then GEOCODE_GROUP
        else substring(GEOCODE_GROUP,12,1)
    end as GEOCODE_GROUP,
    coalesce(
        case
            when trim(GEOCODE_CUSTOM_TEXT) = '2010' then 2010
            when trim(GEOCODE_CUSTOM_TEXT) = '2020' then 2020
            else null
        end,
        case
            when trim(GEO_PROV_REF) = '2010' then 2010
            when trim(GEO_PROV_REF) = '2020' then 2020
            else null
        end,
        0
    ) as geocode_year,
    SHAPEFILE as geocode_shapefile
from 
    SITE_pcornet.private_address_geocode pag
left join 
    (select
		addressid,
	 	patid,
		case
            when address_zip5 is not null then address_zip5
            else address_zip9
        end as zip
    from SITE_pcornet.lds_address_history
	) as lds
    on pag.addressid = lds.addressid
left join 
	SITE_pedsnet.location loc
	on loc.location_source_value like '%patient history%'
	and lds.zip = loc.zip
	and coalesce(pag.GEOCODE_STATE,'') = trim(split_part(loc.location_source_value,'|',3))
	and coalesce(pag.GEOCODE_COUNTY,'') = trim(split_part(loc.location_source_value,'|',4))
	and coalesce(pag.GEOCODE_TRACT ,'') = trim(split_part(loc.location_source_value,'|',5))
	and coalesce(pag.GEOCODE_GROUP ,'') = trim(split_part(loc.location_source_value,'|',6))
;
commit;