create sequence if not exists SITE_pedsnet.imm_seq;

create or replace function is_date(s varchar) returns boolean as $$
begin
          perform s::date;
          return true;
        exception when others then
                  return false;
end;
$$ language plpgsql;

begin;
insert into SITE_pedsnet.immunization(
	imm_body_site_concept_id,
	imm_body_site_source_value, 
	imm_dose_unit_concept_id, 
	imm_dose_unit_source_value, 
	imm_exp_date, 
	imm_exp_datetime, 
	imm_lot_num, 
	imm_manufacturer, 
	imm_recorded_date, 
	imm_recorded_datetime, 
	imm_route_concept_id, 
	imm_route_source_value, 
	immunization_concept_id, 
	immunization_date, 
	immunization_datetime, 
	immunization_dose, 
	immunization_id, 
	immunization_source_concept_id, 
	immunization_source_value, 
	immunization_type_concept_id, 
	person_id, 
	procedure_occurrence_id, 
	provider_id, 
	visit_occurrence_id)
select
	coalesce(
		case
			when imm.vx_body_site = 'BN' then 46233552
			when imm.vx_body_site = 'BU' then 4180330
			when imm.vx_body_site = 'LA' then 46233636
			when imm.vx_body_site = 'LD' then 35632108
			when imm.vx_body_site = 'LG' then 44516524
			when imm.vx_body_site = 'LLAQ' then 45912254
			when imm.vx_body_site = 'LLFA' then 4283512
			when imm.vx_body_site = 'LMFA' then 4284054
			when imm.vx_body_site = 'LT' then 4264018
			when imm.vx_body_site = 'LUA' then 4283159
			when imm.vx_body_site = 'LUAQ' then 45955947
			when imm.vx_body_site = 'LVL' then 4079043
			when imm.vx_body_site = 'NI' then 44814650
			when imm.vx_body_site = 'OT' then 44814649
			when imm.vx_body_site = 'RA' then 44517192
			when imm.vx_body_site = 'RD' then 35632107
			when imm.vx_body_site = 'REJ' then 36695683
			when imm.vx_body_site = 'RF' then 4298982
			when imm.vx_body_site = 'RG' then 35632281
			when imm.vx_body_site = 'RH' then 4302584
			when imm.vx_body_site = 'RLAQ' then 45955948
			when imm.vx_body_site = 'RLFA' then 4271843
			when imm.vx_body_site = 'RMFA' then 4284053
			when imm.vx_body_site = 'RT' then 4008238
			when imm.vx_body_site = 'RUA' then 4274743
			when imm.vx_body_site = 'RUAQ' then 45933908
			when imm.vx_body_site = 'RUFA' then 44517187
			when imm.vx_body_site = 'RVL' then 4031702
			when imm.vx_body_site = 'UN' then 44814653
			else 44814650
		end, 
	44814650) as imm_body_site_concept_id,
	imm.vx_body_site as imm_body_site_source_value, 
	coalesce(unit_map.source_concept_id::int, 44814653) as imm_dose_unit_concept_id, 
	imm.vx_dose_unit as imm_dose_unit_source_value,
    case 
		when is_date(imm.vx_exp_date::varchar) then imm.vx_exp_date::date 
	end as imm_exp_date,
    case 
		when is_date(imm.vx_exp_date::varchar) then imm.vx_exp_date::timestamp 
	end as imm_exp_datetime, 
	imm.vx_lot_num as imm_lot_num, 
	imm.vx_manufacturer as imm_manufacturer, 
	imm.vx_record_date as imm_recorded_date, 
	imm.vx_record_date::timestamp as imm_recorded_datetime, 
	coalesce(route_map.concept_id, 45956875) as imm_route_concept_id, 
	imm.vx_route as imm_route_source_value, 
	coalesce(
		case
            when c_hcpcs.concept_id is not null then c_hcpcs.concept_id
            when imm.vx_code_type='CH' then c_cpt.concept_id
            when imm.vx_code_type='CX' then c_cvx.concept_id
            when imm.vx_code_type='RX' then c_rxnorm.concept_id
            when imm.vx_code_type='ND' then c_ndc.concept_id
            when imm.vx_code_type='NI' then 44814650
            when imm.vx_code_type='UN' then 44814653
            when imm.vx_code_type='OT' then 44814649
      		else 0 
		end, 0) as immunization_concept_id,
	coalesce(imm.vx_admin_date,imm.vx_record_date) as immunization_date,
	coalesce(imm.vx_admin_date,imm.vx_record_date)::timestamp as immunization_datetime,
	imm.vx_dose as dose,
	nextval('SITE_pedsnet.imm_seq') as immunization_id,
	coalesce(	
		case
            when c_hcpcs.concept_id is not null then c_hcpcs.concept_id
            when imm.vx_code_type='CH' then c_cpt.concept_id
            when imm.vx_code_type='CX' then c_cvx.concept_id
            when imm.vx_code_type='RX' then c_rxnorm.concept_id
            when imm.vx_code_type='ND' then c_ndc.concept_id
			when imm.vx_code_type='NI' then 44814650
            when imm.vx_code_type='UN' then 44814653
            when imm.vx_code_type='OT' then 44814649
      else 0 
	end, 0) as immunization_source_concept_id, 
	coalesce(imm.raw_vx_name,'')||'|'||coalesce(imm.vx_code,'') as immunization_source_value, 
	case 
		when vx_source='OD' then 2000001288
		when vx_source='EF' then 2000001289
		when vx_source='IS' then 2000001290
		when vx_source='PR' then 2000001291
		else 44814649
	end as immunization_type_concept_id, 
	person.person_id as person_id,
	po.procedure_occurrence_id as procedure_occurrence_id,
	vo.provider_id as provider_id,
	vo.visit_occurrence_id as visit_occurrence_id
from SITE_pcornet.immunization imm
inner join SITE_pedsnet.person person 
    on imm.patid = person.person_source_value
left join SITE_pedsnet.visit_occurrence vo 
    on imm.encounterid = vo.visit_source_value
left join SITE_pedsnet.procedure_occurrence po
    on imm.proceduresid = po.procedure_source_value
left join vocabulary.concept c_hcpcs
    on imm.vx_code=c_hcpcs.concept_code 
	and imm.vx_code_type='CH' 
	and c_hcpcs.vocabulary_id='HCPCS' 
	and imm.vx_code ~ '[A-Z]'
left join vocabulary.concept c_cpt
    on imm.vx_code=c_cpt.concept_code 
	and imm.vx_code_type='CH' 
	and c_cpt.vocabulary_id='CPT4'
left join vocabulary.concept c_rxnorm
    on imm.vx_code=c_rxnorm.concept_code 
	and imm.vx_code_type='RX' 
	and c_rxnorm.vocabulary_id='RxNorm'
left join vocabulary.concept c_cvx
    on imm.vx_code=c_cvx.concept_code 
	and imm.vx_code_type='CX' 
	and c_cvx.vocabulary_id='CVX'
left join vocabulary.concept c_ndc
    on imm.vx_code=c_ndc.concept_code 
	and imm.vx_code_type='NDC' 
	and c_ndc.vocabulary_id='NDC'
left join pcornet_maps.pedsnet_pcornet_valueset_map unit_map
	on imm.vx_dose_unit = unit_map.target_concept 
	and unit_map.target_concept = 'Dose unit'
left join 
	(
	select target_concept, concept_id
	from 
		(
		select target_concept, source_concept_id
		from pcornet_maps.pedsnet_pcornet_valueset_map
		where source_concept_class = 'Route'
		) as maps
	inner join 
		(
		select concept_id
		from vocabulary.concept
		where standard_concept = 'S' 
		and domain_id = 'Route'
		) as voc
		on maps.source_concept_id = voc.concept_id::varchar
	) as route_map
	on imm.vx_route = route_map.target_concept
;

commit;
