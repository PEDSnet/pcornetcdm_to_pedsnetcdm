begin;
create table SITE_pedsnet.person_primary_provider as
with provider_history as (
    select
	    demo.patid, 
	    enc.providerid, 
	    max((enc.admit_date + enc.admit_time::time)::timestamp) as most_recent_date, 
	    count(enc.providerid) as num_visits
    from SITE_pcornet.demographic demo
    left join SITE_pcornet.encounter enc
    on demo.patid = enc.patid
    group by 
	    demo.patid, 
	    enc.providerid
),
get_provider_max as (
    select 
	    provider_history.patid, 
	    provider_history.providerid, 
	    provider_history.num_visits,
	    provider_history.most_recent_date
    from provider_history
    inner join (
	    select patid, max(num_visits) as max_visit
		from provider_history
		group by patid
	) as most_visits 
	on provider_history.patid = most_visits.patid
	and provider_history.num_visits = most_visits.max_visit
    order by patid, most_recent_date desc
)
select 
	pcor_prov.patid,
	peds_prov.provider_id
from (	
	select patid, max(providerid) as providerid
	from get_provider_max
	group by patid 
) as pcor_prov
inner join SITE_pedsnet.provider peds_prov
	on pcor_prov.providerid = peds_prov.provider_source_value;
commit;