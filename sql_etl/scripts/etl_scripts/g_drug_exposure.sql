create sequence if not exists SITE_pedsnet.drug_exposure_seq;

CREATE OR REPLACE FUNCTION SITE_pedsnet.isnumeric(text) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
        x = $1::NUMERIC;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
                RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION SITE_pedsnet.is_int(text) RETURNS BOOLEAN AS $$
DECLARE x INT;
BEGIN
        x = $1::INT;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
                RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

create or replace function SITE_pedsnet.is_date(s varchar) returns boolean as $$
begin
	  perform s::date;
	  return true;
	exception when others then
		  return false;
end;
$$ language plpgsql;

create or replace function SITE_pedsnet.is_time(s varchar) returns boolean as $$
begin
	  perform s::time;
	  return true;
	exception when others then
		  return false;
end;
$$ language plpgsql;

begin;

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

select distinct
	case 
		when(SITE_pedsnet.isnumeric(dispense_sup::varchar)) then dispense_sup::numeric
	end as days_supply,	
	0 as dispense_as_written_concept_id, 
	coalesce(case 
			when disp.dispense_dose_disp_unit = 'NI' then ucum_maps.source_concept_id::int
			when disp.dispense_dose_disp_unit = 'OT' then ucum_maps.source_concept_id::int
			else unit.concept_id
		end,0) as dose_unit_concept_id,
	dispense_dose_disp_unit as dose_unit_source_value,
	coalesce(ndc_map.concept_id_2,0) drug_concept_id,
	null::date as drug_exposure_end_date,
	null::timestamp as drug_exposure_end_datetime,
	nextval('SITE_pedsnet.drug_exposure_seq') AS drug_exposure_id,
	null::date as drug_exposure_order_date,
	null::timestamp as drug_exposure_order_datetime,
	case
        when dispense_date is null then '0001-01-01'::date
	    else dispense_date::date
	end as drug_exposure_start_date,
	case
	   when dispense_date is null then '0001-01-01'::timestamp
	   else dispense_date::timestamp
	end as drug_exposure_start_datetime,
	--only have concept mappings for 'OD' and 'BI'. Else default to no information
	case
		when dispense_source = 'OD' then 38000275
		when dispense_source = 'BI' then 44786630
		else 44814653 
	end as drug_source_concept_id,
	coalesce(disp.ndc,' ') as drug_source_value,
	38000175 as drug_type_concept_id,
	dispense_dose_disp::varchar as eff_drug_dose_source_value,
	case 
		when(SITE_pedsnet.isnumeric(dispense_dose_disp::varchar)) then dispense_dose_disp::numeric
    end as effective_drug_dose,
	null as frequency,
	null as lot_number,
	person.person_id,
	null as provider_id,
	case when SITE_pedsnet.isnumeric(dispense_amt::varchar)
		then dispense_amt::numeric end as quantity,
	null::integer as refills,
	coalesce(
		case
			when disp.dispense_route = 'OT' then 44814649
			else route.concept_id
		end, 0) as route_concept_id,
	dispense_route as route_source_value,
	null as sig,
	null as stop_reason,
	null as visit_occurrence_id
from 
	SITE_pcornet.dispensing disp
inner join 
	SITE_pedsnet.person person 
    on disp.patid = person.person_source_value
left join 
	vocabulary.concept ndc 
	on disp.ndc=ndc.concept_code 
	and ndc.vocabulary_id='NDC' 
left join 
	vocabulary.concept_relationship ndc_map 
	on ndc.concept_id=ndc_map.concept_id_1 
	and (
		ndc_map.relationship_id='Maps to'
		or ndc_map.relationship_id = 'Non-standard to Standard map (OMOP)'
		)
	and ndc_map.concept_id_2 in (select concept_id from vocabulary.concept where vocabulary_id = 'RxNorm')
left join 
	pcornet_maps.pcornet_pedsnet_valueset_map as ucum_maps
	on disp.dispense_dose_disp_unit = ucum_maps.target_concept 
	and ucum_maps.source_concept_class = 'Dose unit'
left join 
	(select concept_id
	from vocabulary.concept
	where vocabulary_id = 'UCUM' and standard_concept = 'S'
	) as unit
	on ucum_maps.source_concept_id = unit.concept_id::varchar
left join 
	(select target_concept, source_concept_id, concept_id
	 from
		(select target_concept, source_concept_id
		from pcornet_maps.pcornet_pedsnet_valueset_map
		where source_concept_class = 'Route') as route_maps
		inner join (
			select concept_id, vocabulary_id
			from vocabulary.concept
			where domain_id = 'Route' and standard_concept = 'S' 
		) as voc2
		on route_maps.source_concept_id = voc2.concept_id::varchar
	) as route 
	on disp.dispense_route = route.target_concept
;
commit;

begin;
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

select distinct
	case 
		when SITE_pedsnet.is_int(rx_days_supply::varchar) then rx_days_supply::int
	end as days_supply,
	case 
		when rx_dispense_as_written='Y' then 4188539 -- Yes
		when rx_dispense_as_written='N' then 4188540 -- No
		when rx_dispense_as_written='NI' then 44814650 -- No Information
        when rx_dispense_as_written='OT' then 44814649 -- Other
        when rx_dispense_as_written='UN' then 44814653 -- Unknown
		end as dispense_as_written_concept_id,
	coalesce(
		case 
			when presc.rx_dose_ordered_unit = 'NI' then ucum_maps.source_concept_id::int
			when presc.rx_dose_ordered_unit = 'OT' then ucum_maps.source_concept_id::int
			else unit.concept_id
		end, 0) as dose_unit_concept_id,
	rx_dose_ordered_unit as dose_unit_source_value,
	coalesce(rxnorm.concept_id,0) as drug_concept_id,
	rx_end_date::date as drug_exposure_end_date,
	rx_end_date::timestamp as drug_exposure_end_datetime,
	nextval('SITE_pedsnet.drug_exposure_seq') AS drug_exposure_id,
	rx_order_date::date as drug_exposure_order_date,
	case 
		when SITE_pedsnet.is_time(rx_order_time) then (rx_order_date || ' '|| rx_order_time)::timestamp 
		else rx_order_date::timestamp 
	end as drug_exposure_order_datetime,
	case
        when rx_start_date is null then '0001-01-01'::date
        else rx_start_date::date
	end as drug_exposure_start_date,
	case
	    when rx_start_date is null then '0001-01-01'::timestamp
	    else rx_start_date::timestamp
	end as drug_exposure_start_datetime,
	coalesce(rxnorm.concept_id,0) as drug_source_concept_id,
	coalesce(left(raw_rx_med_name, 200),' ')||'|'||coalesce(rxnorm_cui,' ') as drug_source_value,
	38000177 as drug_type_concept_id,
	null as eff_drug_dose_source_value,
	case 
		when(SITE_pedsnet.isnumeric(rx_dose_ordered::varchar)) then rx_dose_ordered::numeric
    end as effective_drug_dose,
	rx_frequency as frequency,
	null as lot_number,
	person.person_id as person_id,
	enc.providerid as provider_id,
	case 
		when SITE_pedsnet.isnumeric(rx_quantity::varchar) then rx_quantity::numeric
	end as quantity,
	case 
		when SITE_pedsnet.is_int(rx_refills::varchar) then rx_refills::int
	end as refills,
	coalesce(case
			when presc.rx_route = 'OT' then 44814649
			else route.concept_id
		end,0) as route_concept_id,
	rx_route as route_source_value,
	null as sig,
	null as stop_reason,
 	enc.encounterid as visit_occurrence_id
from 
	SITE_pcornet.prescribing presc
inner join 
	SITE_pedsnet.person person 
    on presc.patid = person.person_source_value
left join 
	SITE_pcornet.encounter enc
    on presc.encounterid = enc.encounterid
left join 
	vocabulary.concept as rxnorm 
	on trim(presc.rxnorm_cui) = rxnorm.concept_code 
	and vocabulary_id='RxNorm' 
	and standard_concept='S'
left join 
	pcornet_maps.pcornet_pedsnet_valueset_map as ucum_maps
	on presc.rx_dose_ordered_unit = ucum_maps.target_concept 
	and source_concept_class = 'Dose unit'
left join 
	(select concept_id
	from vocabulary.concept
	where vocabulary_id = 'UCUM' and standard_concept = 'S'
	) as unit
	on ucum_maps.source_concept_id = unit.concept_id::varchar
left join 
	(select target_concept, source_concept_id, concept_id
	 from
		(select 
		 target_concept, source_concept_id
		from pcornet_maps.pcornet_pedsnet_valueset_map
		where source_concept_class = 'Route') as route_maps
		inner join (
			select concept_id, vocabulary_id
			from vocabulary.concept
			where domain_id = 'Route' and standard_concept = 'S' 
		) as voc2
		on route_maps.source_concept_id = voc2.concept_id::varchar
	) as route 
	on presc.rx_route = route.target_concept
;

commit;

begin;
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

select distinct
	null::integer as days_supply,
	0 as dispense_as_written_concept_id,
	coalesce(
		case 
			when medadmin.medadmin_dose_admin_unit = 'NI' then ucum_maps.source_concept_id::int
			when medadmin.medadmin_dose_admin_unit = 'OT' then ucum_maps.source_concept_id::int
			else unit.concept_id::int
		end, 0) as dose_unit_concept_id,
	medadmin_dose_admin_unit as dose_unit_source_value,
	coalesce(
		case
			when medadmin_type='ND' then ndc_map.concept_id_2
			when medadmin_type='RX' then rxnorm.concept_id
			else 0 
		end, 0) as drug_concept_id,
	medadmin_stop_date::date as drug_exposure_end_date,
	(medadmin_stop_date || ' '|| medadmin_stop_time)::timestamp as drug_exposure_end_datetime,
 	nextval('SITE_pedsnet.drug_exposure_seq') AS drug_exposure_id,
	null::date as drug_exposure_order_date,
	null::timestamp as drug_exposure_order_datetime,
	case
        when medadmin_start_date is null then '0001-01-01'::date
	    else medadmin_start_date
	end as drug_exposure_start_date,
	case
	    when medadmin_start_date is null OR medadmin_start_time is null then '0001-01-01'::timestamp
        else (medadmin_start_date || ' '|| medadmin_start_time)::timestamp
	end as drug_exposure_start_datetime,
	case
		when medadmin_type='ND' then ndc.concept_id
		when medadmin_type='RX' then rxnorm.concept_id
		else 0 
	end as drug_source_concept_id,
	coalesce(left(raw_medadmin_med_name, 200)||'...',' ')||'|'||coalesce(medadmin_code,' ') as drug_source_value,
	38000180 as drug_type_concept_id,
	medadmin_dose_admin::varchar as eff_drug_dose_source_value,
	case 
		when(SITE_pedsnet.isnumeric(medadmin_dose_admin::varchar)) then medadmin_dose_admin::numeric
    end as effective_drug_dose,
	null as frequency,
	null as lot_number,
	person.person_id as person_id,
	enc.providerid as provider_id,
	null::numeric as quantity,
	null::integer as refills,
	coalesce(
		case 
			when medadmin.medadmin_route = 'OT' then 44814649
			else route.concept_id::int
		end,0) as route_concept_id,
	medadmin_route as route_source_value,
	null as sig,
	null as stop_reason,
	enc.encounterid as visit_occurrence_id
from 
	SITE_pcornet.med_admin as medadmin
inner join 
	SITE_pedsnet.person person 
	on medadmin.patid = person.person_source_value
left join 
	SITE_pcornet.encounter enc
    on medadmin.encounterid = enc.encounterid
left join 
	vocabulary.concept ndc 
	on medadmin.medadmin_code=ndc.concept_code 
	and medadmin_type='ND' 
	and ndc.vocabulary_id='NDC' 
left join 
	vocabulary.concept_relationship ndc_map 
	on ndc.concept_id=ndc_map.concept_id_1 
	and (
		ndc_map.relationship_id='Maps to'
		or ndc_map.relationship_id = 'Non-standard to Standard map (OMOP)'
		)
	and ndc_map.concept_id_2 in (select concept_id from vocabulary.concept where vocabulary_id = 'RxNorm')
left join 
	vocabulary.concept rxnorm 
	on medadmin.medadmin_code = rxnorm.concept_code 
	and medadmin_type='RX' 
	and rxnorm.vocabulary_id='RxNorm' 
	and rxnorm.standard_concept='S'
left join 
	pcornet_maps.pcornet_pedsnet_valueset_map as ucum_maps
	on medadmin.medadmin_dose_admin_unit = ucum_maps.target_concept 
	and ucum_maps.source_concept_class = 'Dose unit'
left join 
	(select concept_id
	from vocabulary.concept
	where vocabulary_id = 'UCUM' and standard_concept = 'S'
	) as unit
	on ucum_maps.source_concept_id = unit.concept_id::varchar
left join 
	(select target_concept, source_concept_id, concept_id
	 from
		(select target_concept, source_concept_id
		from pcornet_maps.pcornet_pedsnet_valueset_map
		where source_concept_class = 'Route') as route_maps
		inner join (
			select concept_id, vocabulary_id
			from vocabulary.concept
			where domain_id = 'Route' and standard_concept = 'S' 
		) as voc2
		on route_maps.source_concept_id = voc2.concept_id::varchar
		where vocabulary_id = 'SNOMED'
	) as route 
	on medadmin.medadmin_route = route.target_concept;

commit;

