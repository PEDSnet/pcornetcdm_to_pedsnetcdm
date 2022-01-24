create sequence SITE_pedsnet.drug_exposure_seq;

begin;

insert into SITE_pedsnet.drug_exposure(
	days_supply, 
	dispense_as_written_concept_id, 
	dose_unit_concept_id, 
	dose_unit_source_value, 
	drug_concept_id, 
	drug_exposure_end_date,
	drug_exposure_end_datetime, x
	drug_exposure_id, 
	drug_exposure_order_date,
	drug_exposure_order_datetime,
	drug_exposure_start_date,
	drug_exposure_start_datetime,
	drug_source_concept_id,
	drug_source_value,
	drug_type_concept_id,
	eff_drug_dose_source_value,
	effective_drug_dose,
	frequency,
	lot_number,
	person_id,
	provider_id, 
	quantity,
	refills,
	route_concept_id,
	route_source_value,
	sig,
	stop_reason,
	visit_occurrence_id )
	
select
	dispense_sup as days_supply,
	0 as dispense_as_written_concept_id, 
	ucum_maps.source_concept_id::integer as dose_unit_concept_id,
	dispense_dose_disp_unit as dose_unit_source_value,
	coalesce(ndc_map.concept_id_2,0) drug_concept_id,
	null as drug_exposure_end_date,
	null as drug_exposure_end_datetime,
	-- nextval('pcornet_pedsnet.drug_exposure_seq')::bigint AS drug_exposure_id,
	null as drug_exposure_order_date,
	null as drug_exposure_order_datetime,
	dispense_date::date as drug_exposure_start_date,
	dispense_date::timestamp as drug_exposure_start_datetime,
	-- coalesce(ndc.concept_id,0) as drug_type_concept_id
	NDC as drug_source_value,
	38000175 as drug_type_concept_id,
	dispense_dose_disp::varchar as eff_drug_dose_source_value,
	dispense_dose_disp as effective_drug_dose,
	null as frequency,
	null as lot_number,
	person.person_id,
	null as provider_id,
	dispense_amt as quantity,
	null as refills,
	voc2.concept_id as route_concept_id,
	dispense_route as route_source_value,
	null as sig,
	null as stop_reason,
	null as visit_occurrence_id
from nationwide_pcornet.dispensing disp
inner join nationwide_pedsnet.person person 
      on disp.patid = person.person_source_value
left join vocabulary.concept ndc on disp.ndc=ndc.concept_code and ndc.vocabulary_id='NDC' and ndc.invalid_reason is null
left join vocabulary.concept_relationship ndc_map on ndc.concept_id=ndc_map.concept_id_1 and ndc_map.relationship_id='Maps to'
left join pcornet_maps.pedsnet_pcornet_valueset_map as ucum_maps
	on disp.dispense_dose_disp_unit = ucum_maps.target_concept and ucum_maps.source_concept_class = 'Dose unit'
inner join vocabulary.concept voc1 on ucum_maps.source_concept_id = CAST(voc1.concept_id as varchar) and voc1.vocabulary_id = 'UCUM' and voc1.standard_concept = 'S'
left join pcornet_maps.pedsnet_pcornet_valueset_map as route_maps
	on disp.dispense_route = route_maps.target_concept and route_maps.source_concept_class = 'Route'
inner join vocabulary.concept voc2 on route_maps.source_concept_id = CAST(voc2.concept_id as varchar) and voc2.domain_id = 'Route'

;

commit;

insert into SITE_pedsnet.drug_exposure(
	days_supply,
	dispense_as_written_concept_id,
	dose_unit_concept_id,
	dose_unit_source_value,
	drug_concept_id,
	drug_exposure_end_date,
	drug_exposure_end_datetime,
	drug_exposure_id,
	drug_exposure_order_date,
	drug_exposure_order_datetime,
	drug_exposure_start_date,
	drug_exposure_start_datetime,
	drug_source_concept_id,
	drug_source_value,
	drug_type_concept_id,
	eff_drug_dose_source_value,
	effective_drug_dose,
	frequency,
	lot_number,
	person_id,
	provider_id,
	quantity,
	refills,
	route_concept_id,
	route_source_value,
	sig,
	stop_reason,
	visit_occurrence_id)

select
	rx_days_supply as days_supply,
	case 
		when rx_dispense_as_written='Y' then 4188539 -- Yes
		when rx_dispense_as_written='N' then 4188540 -- No
		when rx_dispense_as_written='NI' then 444814650 -- No Information
        when rx_dispense_as_written='OT' then 44814649 -- Other
        when rx_dispense_as_written='UN' then 44814653 -- Unknown
		end as dispense_as_written_concept_id,
	0 as dose_unit_concept_id,
	rx_dose_ordered_unit as dose_unit_source_value,
	coalesce(rxnorm.concept_id,0) as drug_concept_id,
	rx_end_date::date as drug_exposure_end_date,
	rx_end_date::timestamp as drug_exposure_end_datetime,
	nextval('pcornet_pedsnet.drug_exposure_seq')::bigint AS drug_exposure_id,
	rx_order_date::date as drug_exposure_order_date,
	(rx_order_date || ' '|| rx_order_time)::timestamp as drug_exposure_order_datetime,
	rx_start_date::date as drug_exposure_start_date,
	rx_start_date::timestamp as drug_exposure_start_datetime,
	coalesce(rxnorm.concept_id,0) as drug_source_concept_id,
	coalesce(raw_rx_med_name,' ')|'|'||coalesce(rxnrom_cui,' ') as drug_source_value,
	38000177 as drug_type_concept_id,
	null as eff_drug_dose_source_value,
	rx_dose_ordered as effective_drug_dose,
	rx_frequency as frequency,
	null as lot_number,
	person.person_id as person_id,
	vo.provider_id as provider_id,
	rx_quantity as quantity,
	rx_refills as refills,
	0 as route_concept_id,
	rx_route as route_source_value,
	null as sig,
	null as stop_reason,
	vo.visit_occurrence_id as visit_occurrence_id
from SITE_pcornet.prescribing presc
inner join SITE_pedsnet.person person 
      on presc.patid = person.person_source_value
inner join SITE_pedsnet.visit_occurrence vo 
      on presc.encounterid = vo.visit_source_value
left join vocabulary.concept rxnorm as presc.rxnorm_cui=rxnorm.concept.code and vocabulary_id='RxNorm' and standard_concept='S'
;

commit;

insert into SITE_pedsnet.drug_exposure(
	days_supply,
	dispense_as_written_concept_id, 
	dose_unit_concept_id, 
	dose_unit_source_value, 
	drug_concept_id, 
	drug_exposure_end_date, 
	drug_exposure_end_datetime, 
	drug_exposure_id, 
	drug_exposure_order_date, 
	drug_exposure_order_datetime, 
	drug_exposure_start_date, 
	drug_exposure_start_datetime, 
	drug_source_concept_id, 
	drug_source_value, 
	drug_type_concept_id, 
	eff_drug_dose_source_value, 
	effective_drug_dose,
	frequency, 
	lot_number, 
	person_id, 
	provider_id, 
	quantity,
	refills,
	route_concept_id, 
	route_source_value,
	sig, 
	stop_reason, 
	visit_occurrence_id)

select
	null as days_supply,
	0 as dispense_as_written_concept_id,
	ucum_maps.source_concept_id::integer as dose_unit_concept_id,
	medadmin_dose_admin_unit as dose_unit_source_value,
	case
		when medadmin_type='ND' then ndc_map.concept_id_2
		when medadmin_type='RX' then rxnorm.concept_id
		else 0 end as drug_concept_id,
	medadmin_stop_date::date as drug_exposure_end_date,
	(medadmin_stop_date || ' '|| medadmin_stop_time)::timestamp as drug_exposure_end_datetime,
-- 	nextval('pcornet_pedsnet.drug_exposure_seq')::bigint AS drug_exposure_id,
	null as drug_exposure_order_date,
	null as drug_exposure_order_datetime,
	medadmin_start_date as drug_exposure_start_date,
	(medadmin_start_date || ' '|| medadmin_start_time)::timestamp as drug_exposure_start_datetime,
	case
		when medadmin_type='ND' then ndc.concept_id
		when medadmin_type='RX' then rxnorm.concept_id
		else 0 end as drug_source_concept_id,
	coalesce(raw_medadmin_med_name,' ')||'|'||coalesce(medadmin_code,' ') as drug_source_value,
	38000180 as drug_type_concept_id,
	medadmin_dose_admin::varchar as eff_drug_dose_source_value,
	medadmin_dose_admin as effective_drug_dose,
	null as frequency,
	null as lot_number,
	person.person_id as person_id,
	-- vo.provider_id as provider_id,
	medadmin.medadmin_providerid::bigint as provider_id,
	null as quantity,
	null as refills,
	voc2.concept_id as route_concept_id,
	medadmin_route as route_source_value,
	null as sig,
	null as stop_reason,
	-- vo.visit_occurrence_id as visit_occurrence_id
	medadmin.encounterid::bigint as visit_occurrence_id
from colorado_pcornet.med_admin as medadmin
inner join colorado_pedsnet.person person 
on medadmin.patid = person.person_source_value
-- inner join colorado_pedsnet.visit_occurrence vo 
--       on med.encounterid = vo.visit_occurrence_id::varchar
left join vocabulary.concept ndc on medadmin.medadmin_code=ndc.concept_code and medadmin_type='ND' and ndc.vocabulary_id='NDC' and ndc.invalid_reason is null
left join vocabulary.concept_relationship ndc_map on ndc.concept_id=ndc_map.concept_id_1 and ndc_map.relationship_id='Maps to'
left join vocabulary.concept rxnorm on medadmin.medadmin_code = rxnorm.concept_code and medadmin_type='RX' and rxnorm.vocabulary_id='RxNorm' and rxnorm.standard_concept='S'
left join pcornet_maps.pedsnet_pcornet_valueset_map as ucum_maps
	on medadmin.medadmin_dose_admin_unit = ucum_maps.target_concept and ucum_maps.source_concept_class = 'Dose unit'
inner join vocabulary.concept voc1 on ucum_maps.source_concept_id = CAST(voc1.concept_id as varchar) and voc1.vocabulary_id = 'UCUM' and voc1.standard_concept = 'S'
left join pcornet_maps.pedsnet_pcornet_valueset_map as route_maps
	on medadmin.medadmin_route = route_maps.target_concept and route_maps.source_concept_class = 'Route'
inner join vocabulary.concept voc2 on route_maps.source_concept_id = CAST(voc2.concept_id as varchar) and voc2.domain_id = 'Route'


commit;
