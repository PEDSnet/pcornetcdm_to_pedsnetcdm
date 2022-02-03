begin;

insert into SITE_pedsnet.visit_payer(
	plan_class, 
	plan_name, 
	plan_type, 
	visit_occurrence_id, 
	visit_payer_id, 
	visit_payer_type_concept_id,
	site)
with payer_info as (
select
	split_part(source_concept_id,'-',1) as plan_class,
	split_part(source_concept_id,'-',2)as plan_type,
	coalesce(raw_payer_name_primary,source_concept_id) as plan_name,
	vo.visit_occurrence_id,
	31968::int as visit_payer_type_concept_id -- primary
from SITE_pcornet.encounter enc
inner join SITE_pedsnet.visit_occurrence vo 
	on enc.encounterid=vo.visit_source_value
left join pcornet_maps.pedsnet_pcornet_valueset_map payer_map
	on enc.payer_type_primary=payer_map.target_concept
	and payer_map.source_concept_class='Payer'
where payer_type_primary is not null 
	and payer_type_primary <>'NI'
union
select
	split_part(source_concept_id,'-',1) as plan_class,
	split_part(source_concept_id,'-',2)as plan_type,
	coalesce(raw_payer_name_secondary,source_concept_id) as plan_name,
	vo.visit_occurrence_id,
	31969::int as visit_payer_type_concept_id -- secondary
from SITE_pcornet.encounter enc
inner join SITE_pedsnet.visit_occurrence vo 
	on enc.encounterid=vo.visit_source_value
left join pcornet_maps.pedsnet_pcornet_valueset_map payer_map
	on enc.payer_type_secondary=payer_map.target_concept
	and payer_map.source_concept_class='Payer'
where payer_type_secondary is not null 
	and payer_type_secondary <>'NI'
)
select
	coalesce(payer_info.plan_class,'Other/Unknown') as plan_class,
	coalesce(payer_info.plan_type,'Other/Unknown') as plan_type,
	payer_info.plan_name,
	row_number() over (order by visit_occurrence_id)::bigint as visit_payer_id,
	payer_info.visit_occurrence_id,
	payer_info.visit_payer_type_concept_id,
	'SITE' as site
from payer_info;

commit;