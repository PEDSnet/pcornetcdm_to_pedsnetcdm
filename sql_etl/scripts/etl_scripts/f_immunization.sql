create sequence if not exists SITE_pedsnet.imm_seq;


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
	0 as imm_body_site_concept_id,
	imm.vx_body_site as imm_body_site_source_value, 
	0 as imm_dose_unit_concept_id, 
	imm.vx_dose_unit as imm_dose_unit_source_value, 
	imm.vx_exp_date as imm_exp_date, 
	imm.vx_exp_date::timestamp as imm_exp_datetime, 
	imm.vx_lot_num as imm_lot_num, 
	imm.vx_manufacturer as imm_manufacturer, 
	imm.vx_record_date as imm_recorded_date, 
	imm.vx_record_date::timestamp as imm_recorded_datetime, 
	0 as imm_route_concept_id, 
	imm.vx_route as imm_route_source_value, 
		case
            when c_hcpcs.concept_id is not null then c_hcpcs.concept_id
            when imm.vx_code_type='CH' then c_cpt.concept_id
            when imm.vx_code_type='CX' then c_cvx.concept_id
            when imm.vx_code_type='RX' then c_rxnorm.concept_id
            when imm.vx_code_type='ND' then c_ndc.concept_id
      else 0 
	end as immunization_concept_id,
	coalesce(imm.vx_admin_date,imm.vx_record_date) as immunization_date,
	coalesce(imm.vx_admin_date,imm.vx_record_date)::timestamp as immunization_datetime,
	imm.vx_dose as dose,
	nextval('SITE_pedsnet.imm_seq')::bigint as immunization_id,
		case
            when c_hcpcs.concept_id is not null then c_hcpcs.concept_id
            when imm.vx_code_type='CH' then c_cpt.concept_id
            when imm.vx_code_type='CX' then c_cvx.concept_id
            when imm.vx_code_type='RX' then c_rxnorm.concept_id
            when imm.vx_code_type='ND' then c_ndc.concept_id
      else 0 
	end as immunization_source_concept_id, 
	coalesce(imm.raw_vx_name,'')||'|'||coalesce(imm.raw_vx_code,'') as immunization_source_value, 
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
inner join SITE_pedsnet.visit_occurrence vo 
      on imm.encounterid = vo.visit_source_value
inner join SITE_pedsnet.procedure_occurrence po
      on imm.proceduresid = po.procedure_source_value
left join vocabulary.concept c_hcpcs
      on imm.vx_code=c_hcpcs.concept_code and imm.vx_code_type='CH' and c_hcpcs.vocabulary_id='HCPCS' and imm.vx_code ~ '[A-Z]'
left join vocabulary.concept c_cpt
      on imm.vx_code=c_cpt.concept_code and imm.vx_code_type='CH' and c_cpt.vocabulary_id='CPT4'
left join vocabulary.concept c_rxnorm
      on imm.vx_code=c_rxnorm.concept_code and imm.vx_code_type='RX' and c_rxnorm.vocabulary_id='RxNorm'
left join vocabulary.concept c_cvx
      on imm.vx_code=c_cvx.concept_code and imm.vx_code_type='CX' and c_cvx.vocabulary_id='CVX'
 left join vocabulary.concept c_ndc
      on imm.vx_code=c_ndc.concept_code and imm.vx_code_type='NDC' and c_ndc.vocabulary_id='NDC';

commit;