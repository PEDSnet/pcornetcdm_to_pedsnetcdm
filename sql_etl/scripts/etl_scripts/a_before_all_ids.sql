ALTER TABLE SITE_pedsnet.adt_occurrence
	ALTER COLUMN adt_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.adt_occurrence
	ALTER COLUMN next_adt_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.adt_occurrence
	ALTER COLUMN prior_adt_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.adt_occurrence
	ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);   

ALTER TABLE SITE_pedsnet.adt_occurrence
	ALTER COLUMN person_id TYPE CHARACTER VARYING(256);   

ALTER TABLE SITE_pedsnet.adt_occurrence
	ALTER COLUMN care_site_id TYPE CHARACTER VARYING(256);   

ALTER TABLE SITE_pedsnet.care_site     
    ALTER COLUMN care_site_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.care_site
    ALTER COLUMN location_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.condition_occurrence
    ALTER COLUMN condition_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.condition_occurrence
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.condition_occurrence
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.death
    ALTER COLUMN death_cause_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.drug_exposure
    ALTER COLUMN drug_exposure_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.drug_exposure
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.drug_exposure
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.device_exposure
    ALTER COLUMN device_exposure_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.device_exposure
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.device_exposure
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.device_exposure
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.immunization
    ALTER COLUMN immunization_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.immunization
    ALTER COLUMN procedure_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.immunization
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.immunization
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.location    
    ALTER COLUMN location_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.location_history
    ALTER COLUMN location_history_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.location_history
    ALTER COLUMN entity_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.location_history
    ALTER COLUMN location_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.measurement     
    ALTER COLUMN measurement_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.measurement
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.measurement_organism
    ALTER COLUMN meas_organism_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.measurement     
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.observation       
    ALTER COLUMN observation_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.observation
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.observation
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.observation_period
    ALTER COLUMN observation_period_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.person            
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.person
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.death
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.hash_token
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.condition_occurrence
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.drug_exposure
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.measurement
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.immunization
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.observation
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.procedure_occurrence
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.visit_occurrence
    ALTER COLUMN person_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.procedure_occurrence
    ALTER COLUMN procedure_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.procedure_occurrence
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.procedure_occurrence
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.provider            
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.visit_occurrence
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.visit_occurrence
    ALTER COLUMN care_site_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.visit_occurrence
    ALTER COLUMN provider_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.visit_payer     
    ALTER COLUMN visit_payer_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.visit_payer
    ALTER COLUMN visit_occurrence_id TYPE CHARACTER VARYING(256);

-- ALTER TABLE SITE_pedsnet.condition_era
--     ALTER COLUMN condition_era_id TYPE CHARACTER VARYING(256);

-- ALTER TABLE SITE_pedsnet.dose_era
--     ALTER COLUMN dose_era_id TYPE CHARACTER VARYING(256);

-- ALTER TABLE SITE_pedsnet.drug_era
--     ALTER COLUMN drug_era_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.location_fips
    ALTER COLUMN geocode_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.location_fips
    ALTER COLUMN location_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.specialty
    ALTER COLUMN entity_id TYPE CHARACTER VARYING(256);

ALTER TABLE SITE_pedsnet.specialty
    ALTER COLUMN specialty_id TYPE CHARACTER VARYING(256);