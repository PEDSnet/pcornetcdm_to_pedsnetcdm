create sequence if not exists SITE_pedsnet.imm_seq;
-- pull from pcornet.immunization table
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
select distinct
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
		when SITE_pedsnet.is_date(imm.vx_exp_date::varchar) then imm.vx_exp_date::date 
	end as imm_exp_date,
    case 
		when SITE_pedsnet.is_date(imm.vx_exp_date::varchar) then imm.vx_exp_date::timestamp 
	end as imm_exp_datetime, 
	imm.vx_lot_num as imm_lot_num, 
	imm.vx_manufacturer as imm_manufacturer, 
	imm.vx_record_date as imm_recorded_date, 
	imm.vx_record_date::timestamp as imm_recorded_datetime, 
	coalesce(route_map.concept_id, 45956875) as imm_route_concept_id, 
	imm.vx_route as imm_route_source_value, 
	coalesce(
		case
			when imm.VX_CODE = '207' then 724906::int
			when imm.VX_CODE = '208' then 724907::int
			when imm.VX_CODE = '210' then 724905::int
			when imm.VX_CODE = '213' then 724904::int
			when imm.VX_CODE = '212' then 702866::int
			when imm.VX_CODE = '217' then 702677::int
			when imm.VX_CODE = '218' then 702678::int
			when imm.VX_CODE = '211' then 702679::int
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
	coalesce(imm.vx_admin_date,imm.vx_record_date,'0001-01-01')::date as immunization_date,
	coalesce(imm.vx_admin_date,imm.vx_record_date,'0001-01-01')::timestamp as immunization_datetime,
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
	po.proceduresid as procedure_occurrence_id,
	enc.providerid as provider_id,
	enc.encounterid as visit_occurrence_id
from 
	SITE_pcornet.immunization imm
inner join 
	SITE_pedsnet.person person 
    on imm.patid = person.person_source_value
left join 
	SITE_pcornet.encounter enc
    on imm.encounterid = enc.encounterid
left join 
	SITE_pcornet.procedures po
    on imm.proceduresid = po.proceduresid
left join 
	vocabulary.concept c_hcpcs
    on imm.vx_code=c_hcpcs.concept_code 
	and imm.vx_code_type='CH' 
	and c_hcpcs.vocabulary_id='HCPCS' 
	and imm.vx_code ~ '[A-Z]'
left join 
	vocabulary.concept c_cpt
    on imm.vx_code=c_cpt.concept_code 
	and imm.vx_code_type='CH' 
	and c_cpt.vocabulary_id='CPT4'
left join 
	vocabulary.concept c_rxnorm
    on imm.vx_code=c_rxnorm.concept_code 
	and imm.vx_code_type='RX' 
	and c_rxnorm.vocabulary_id='RxNorm'
left join 
	vocabulary.concept c_cvx
    on imm.vx_code=c_cvx.concept_code 
	and imm.vx_code_type='CX' 
	and c_cvx.vocabulary_id='CVX'
left join 
	vocabulary.concept c_ndc
    on imm.vx_code=c_ndc.concept_code 
	and imm.vx_code_type='ND' 
	and c_ndc.vocabulary_id='NDC'
left join 
	pcornet_maps.pcornet_pedsnet_valueset_map unit_map
	on imm.vx_dose_unit = unit_map.target_concept 
	and unit_map.target_concept = 'Dose unit'
left join 
	(
	select target_concept, concept_id
	from 
		(
		select target_concept, source_concept_id
		from pcornet_maps.pcornet_pedsnet_valueset_map
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
	on imm.vx_route = route_map.target_concept;
commit;

-- update covid immunization concepts that are not in CVX to CVX
begin;
update SITE_pedsnet.immunization
set immunization_concept_id = 
	case 
		when immunization_concept_id = 42639775 then 724904
		when immunization_concept_id = 42639776 then 724904
		when immunization_concept_id = 42639777 then 724904
		when immunization_concept_id = 42639778 then 724904
		when immunization_concept_id = 42639779 then 724904
		when immunization_concept_id = 42639780 then 724904
		when immunization_concept_id = 37003432 then 724904
		when immunization_concept_id = 37003435 then 724904
		when immunization_concept_id = 37003436 then 724907
		when immunization_concept_id = 37003518 then 724906
		when immunization_concept_id = 739902 then 724904
		when immunization_concept_id = 739906 then 702866
		when immunization_concept_id = 702118 then 724904
		when immunization_concept_id = 1759206 then 724904
		when immunization_concept_id = 766238 then 724907
		when immunization_concept_id = 766239 then 724906
		when immunization_concept_id = 766241 then 702866
		when immunization_concept_id = 766240 then 724905
		when immunization_concept_id = 766234 then 724906
		when immunization_concept_id = 766235 then 702866
		when immunization_concept_id = 766236 then 724905
		when immunization_concept_id = 766231 then 724907
		when immunization_concept_id = 766232 then 724907
		when immunization_concept_id = 766233 then 724906
		when immunization_concept_id = 759694 then 724904
		when immunization_concept_id = 759738 then 724904
		when immunization_concept_id = 759726 then 724907
		when immunization_concept_id = 759728 then 724904
		when immunization_concept_id = 759729 then 724904
		when immunization_concept_id = 759735 then 702679
		when immunization_concept_id = 759736 then 724907
		when immunization_concept_id = 759725 then 724907
		when immunization_concept_id = 759450 then 724904
		when immunization_concept_id = 759451 then 724904
		when immunization_concept_id = 759452 then 724904
		when immunization_concept_id = 759453 then 724904
		when immunization_concept_id = 759454 then 724904
		when immunization_concept_id = 759430 then 724907
		when immunization_concept_id = 759431 then 724907
		when immunization_concept_id = 759433 then 724907
		when immunization_concept_id = 759434 then 724907
		when immunization_concept_id = 759435 then 724906
		when immunization_concept_id = 759428 then 702679
	end 
where 
	immunization_concept_id in
	(
		42639775,
		42639776,
		42639777,
		42639778,
		42639779,
		42639780,
		37003432,
		37003435,
		37003436,
		37003518,
		739902,
		739906,
		702118,
		1759206,
		766238,
		766239,
		766241,
		766240,
		766234,
		766235,
		766236,
		766231,
		766232,
		766233,
		759694,
		759738,
		759726,
		759728,
		759729,
		759735,
		759736,
		759725,
		759450,
		759451,
		759452,
		759453,
		759454,
		759430,
		759431,
		759433,
		759434,
		759435,
		759428
	);
commit;

-- pull covid vaccine concepts from procedure_occurrence
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
select distinct
	0 as imm_body_site_concept_id,
	null as imm_body_site_source_value,
	null::integer as imm_dose_unit_concept_id, 
	null as imm_dose_unit_source_value, 
	null::date as imm_exp_date, 
	null::timestamp as imm_exp_datetime, 
	null as imm_lot_num, 
	null as imm_manufacturer, 
	po.procedure_date as imm_recorded_date,
	po.procedure_datetime as imm_recorded_datetime,
	null::integer as imm_route_concept_id,
	null as imm_route_source_value,
	case
		when po.procedure_concept_id = 42639775 then 724904
		when po.procedure_concept_id = 42639776 then 724904
		when po.procedure_concept_id = 42639777 then 724904
		when po.procedure_concept_id = 42639778 then 724904
		when po.procedure_concept_id = 42639779 then 724904
		when po.procedure_concept_id = 42639780 then 724904
		when po.procedure_concept_id = 37003436 then 724907
		when po.procedure_concept_id = 37003518 then 724906
		when po.procedure_concept_id = 766238 then 724907
		when po.procedure_concept_id = 766239 then 724906
		when po.procedure_concept_id = 766241 then 702866
		when po.procedure_concept_id = 766234 then 724906
		when po.procedure_concept_id = 766235 then 702866
		when po.procedure_concept_id = 766236 then 724905
		when po.procedure_concept_id = 766231 then 724907
		when po.procedure_concept_id = 766232 then 724907
		when po.procedure_concept_id = 766233 then 724906
		when po.procedure_concept_id = 759694 then 724904
		when po.procedure_concept_id = 759738 then 724904
		when po.procedure_concept_id = 759726 then 724907
		when po.procedure_concept_id = 759728 then 724904
		when po.procedure_concept_id = 759729 then 724904
		when po.procedure_concept_id = 759735 then 702679
		when po.procedure_concept_id = 759736 then 724907
		when po.procedure_concept_id = 759725 then 724907
		when po.procedure_concept_id = 759450 then 724904
		when po.procedure_concept_id = 759451 then 724904
		when po.procedure_concept_id = 759452 then 724904
		when po.procedure_concept_id = 759453 then 724904
		when po.procedure_concept_id = 759454 then 724904
		when po.procedure_concept_id = 759430 then 724907
		when po.procedure_concept_id = 759431 then 724907
		when po.procedure_concept_id = 759433 then 724907
		when po.procedure_concept_id = 759434 then 724907
		when po.procedure_concept_id = 759435 then 724906
		when po.procedure_concept_id = 759428 then 702679
		else 0
	end as immunization_concept_id,
	po.procedure_date as immunization_date, 
	po.procedure_datetime as immunization_datetime,
	null as immunization_dose,
	nextval('SITE_pedsnet.imm_seq') as immunization_id,
	po.procedure_concept_id as immunization_source_concept_id,
	'procedure'||'|'||coalesce(po.procedure_source_value,'') as immunization_source_value, 
	2000001288 as immunization_type_concept_id, 
	person.person_id as person_id,
	po.procedure_occurrence_id as procedure_occurrence_id,
	po.provider_id as provider_id,
	po.visit_occurrence_id as visit_occurrence_id
from 
	SITE_pedsnet.procedure_occurrence po
inner join 
	SITE_pedsnet.person person 
    on po.person_id = person.person_id
left join 
	SITE_pedsnet.visit_occurrence vo 
    on po.visit_occurrence_id = vo.visit_occurrence_id
where 
	po.procedure_occurrence_id in
	(
		'42639775',
		'42639776',
		'42639777',
		'42639778',
		'42639779',
		'42639780',
		'37003436',
		'37003518',
		'766238',
		'766239',
		'766241',
		'766234',
		'766235',
		'766236',
		'766231',
		'766232',
		'766233',
		'759694',
		'759738',
		'759726',
		'759728',
		'759729',
		'759735',
		'759736',
		'759725',
		'759450',
		'759451',
		'759452',
		'759453',
		'759454',
		'759430',
		'759431',
		'759433',
		'759434',
		'759435',
		'759428'
	);
commit;

-- pull covid vaccine concepts from drug_exposure 
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
select distinct
	0 as imm_body_site_concept_id, 
	null as imm_body_site_source_value, 
	dose_unit_concept_id as imm_dose_unit_concept_id, 
	dose_unit_source_value as imm_dose_unit_source_value, 
	null::date as imm_exp_date, 
	null::timestamp as imm_exp_datetime, 
	lot_number as imm_lot_num, 
	null as imm_manufacturer, 
	drug_exposure_start_date as imm_recorded_date, 
	drug_exposure_start_datetime as imm_recorded_datetime, 
	route_concept_id as imm_route_concept_id, 
	route_source_value as imm_route_source_value, 
	case
		when drug_concept_id = 37003432 then 724904
		when drug_concept_id = 37003435 then 724904
		when drug_concept_id = 37003436 then 724907
		when drug_concept_id = 37003518 then 724906
		when drug_concept_id = 739902 then 724904
		when drug_concept_id = 739906 then 702866
		when drug_concept_id = 702118 then 724904
		when drug_concept_id = 1759206 then 724904
		else 0
	end as immunization_concept_id, 
	drug_exposure_start_date as immunization_date, 
	drug_exposure_start_datetime as immunization_datetime, 
	effective_drug_dose as immunization_dose, 
	nextval('SITE_pedsnet.imm_seq') as immunization_id, 
	drug_concept_id as immunization_source_concept_id, 
	drug_source_value as immunization_source_value, 
	44814650 as immunization_type_concept_id, 
	person.person_id as person_id,
	null as procedure_occurrence_id,
	vo.provider_id as provider_id,
	vo.visit_occurrence_id as visit_occurrence_id
from 
	SITE_pedsnet.drug_exposure de
inner join 
	SITE_pedsnet.person person 
    on de.person_id = person.person_id
left join 
	SITE_pedsnet.visit_occurrence vo 
    on de.visit_occurrence_id = vo.visit_occurrence_id
where
	drug_concept_id in 
	(
		37003432,
		37003435,
		37003436,
		37003518,
		739902,
		739906,
		702118,
		1759206
	);
commit;
