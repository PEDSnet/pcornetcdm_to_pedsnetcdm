INSERT INTO SITE_pedsnet.visit_occurrence ( visit_occurrence_id, admitted_from_concept_id,
	admitted_from_source_value, care_site_id, discharge_to_concept_id, discharge_to_source_value, 
	person_id, preceding_visit_occurrence_id, provider_id, visit_concept_id, visit_end_date, 
	visit_end_datetime, visit_source_concept_id, visit_source_value, 
	visit_start_date, visit_start_datetime, visit_type_concept_id, domain_source, site)
SELECT distinct on (encounterid) encounterid::int AS visit_occurrence_id,
vsrc.target_concept_id AS admitted_from_concept_id,
enc.raw_admitting_source AS admitted_from_source_value,
enc.facilityid::int as care_site_id,
disp.target_concept_id    AS discharge_to_concept_id,
enc.raw_discharge_status AS discharge_to_source_value,
patid::int   AS person_id,
NULL AS preceding_visit_occurrence_id, ---
enc.providerid::int AS provider_id,
vx.target_concept_id  AS visit_concept_id,
coalesce(enc.discharge_date, enc.admit_date) AS visit_end_date,
coalesce(enc.discharge_date, enc.admit_date) AS visit_end_datetime,
NULL AS visit_source_concept,
enc.raw_enc_type AS visit_source_value,
enc.admit_date AS visit_start_date,
enc.admit_date AS visit_start_datetime,
coalesce( typ.source_concept_id::int, 32035) AS visit_type_concept_id, ---- where did the record came from / need clarification from SME
'PCORNET_ENCOUNTER' AS domain_source,
'SITE' as site
FROM SITE_pcornet.encounter enc
LEFT JOIN cdmh_staging.p2o_facility_type_xwalk ftx ON ftx.cdm_source = 'PCORnet'
                                                  AND ftx.cdm_tbl = 'ENCOUNTER'
                                                  AND ftx.src_facility_type = enc.facility_type
LEFT JOIN cdmh_staging.visit_xwalk vx ON vx.cdm_tbl = 'ENCOUNTER'
                                      AND vx.cdm_name = 'PCORnet'
                                      AND vx.src_visit_type = coalesce(TRIM(enc.enc_type), 'UN')
LEFT JOIN cdmh_staging.p2o_admitting_source_xwalk vsrc ON vx.cdm_tbl = 'ENCOUNTER'
                                                       AND vx.cdm_name = 'PCORnet'
                                                        AND vsrc.src_admitting_source_type = enc.admitting_source
LEFT JOIN cdmh_staging.p2o_discharge_status_xwalk  disp ON disp.cdm_tbl = 'ENCOUNTER'
                                                    AND disp.cdm_source = 'PCORnet'
                                                    AND disp.src_discharge_status = enc.discharge_status
left join pcornet_maps.pedsnet_pcornet_valueset_map typ on typ.target_concept = enc.enc_type and typ.source_concept_class = 'Encounter type';
													
													
													