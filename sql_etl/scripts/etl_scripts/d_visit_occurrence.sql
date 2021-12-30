begin;

INSERT INTO SITE_pedsnet.visit_occurrence ( 
	visit_occurrence_id,
	admitted_from_concept_id,
	admitted_from_source_value,
	care_site_id,
	discharge_to_concept_id,
	discharge_to_source_value, 
	person_id,
	preceding_visit_occurrence_id,
	provider_id, 
	visit_concept_id, 
	visit_end_date, 
	visit_end_datetime, 
	visit_source_concept_id, 
	visit_source_value, 
	visit_start_date, 
	visit_start_datetime, 
	visit_type_concept_id, 
	site)
SELECT 
	distinct on (encounterid) encounterid::bigint AS visit_occurrence_id,
	vsrc.target_concept_id AS admitted_from_concept_id,
	enc.raw_admitting_source AS admitted_from_source_value,
	enc.facilityid::bigint as care_site_id,
	disp.target_concept_id    AS discharge_to_concept_id,
	enc.raw_discharge_status AS discharge_to_source_value,
	person.person_id   AS person_id,
	NULL AS preceding_visit_occurrence_id,
	enc.providerid::bigint AS provider_id,
	typ.source_concept_id AS visit_concept_id,
	coalesce(enc.discharge_date, enc.admit_date) AS visit_end_date,
	coalesce(enc.discharge_date, enc.admit_date)::timestamp AS visit_end_datetime,
	0 AS visit_source_concept_id,
	enc.encounterid AS visit_source_value,
	enc.admit_date AS visit_start_date,
	(enc.admit_date)::timestamp AS visit_start_datetime,
	44818518 AS visit_type_concept_id,
	'SITE' as site
FROM SITE_pcornet.encounter enc
inner join SITE_pedsnet.person person on enc.patid=person.person_source_value
LEFT JOIN cdmh_staging.p2o_admitting_source_xwalk vsrc ON vx.cdm_tbl = 'ENCOUNTER'
                                                       AND vx.cdm_name = 'PCORnet'
                                                        AND vsrc.src_admitting_source_type = enc.admitting_source
LEFT JOIN cdmh_staging.p2o_discharge_status_xwalk  disp ON disp.cdm_tbl = 'ENCOUNTER'
                                                    AND disp.cdm_source = 'PCORnet'
                                                    AND disp.src_discharge_status = enc.discharge_status
left join pcornet_maps.pedsnet_pcornet_valueset_map typ 
				on typ.target_concept = enc.enc_type 
				and typ.source_concept_class = 'Encounter type'
				and source_concept_id not in ('2000000469','42898160');											
commit;													
													