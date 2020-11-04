CREATE TABLE CDMH_STAGING.ETHNICITY_XWALK
(
  CDM_NAME character varying(100 ) 
, CDM_TBL character varying(100 ) 
, SRC_ETHNICITY character varying(100 ) 
, FHIR_CD character varying(100 ) 
, TARGET_CONCEPT_ID integer NOT NULL 
, TARGET_CONCEPT_NAME character varying(255 ) NOT NULL 
, TARGET_DOMAIN_ID character varying(20 ) NOT NULL 
, TARGET_VOCABULARY_ID character varying(20 ) NOT NULL 
, TARGET_CONCEPT_CLASS_ID character varying(20 ) NOT NULL 
, TARGET_STANDARD_CONCEPT character varying(1 ) 
, TARGET_CONCEPT_CODE character varying(50 ) NOT NULL 
) 
;
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('I2B2ACT','PATIENT_DIMENSION','DEM|HISP:NI','No Information',0,'No Information','Ethnicity','Ethnicity','Ethnicity','S','NI');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','DEMOGRAPHIC','Y','2135-2',38003563,'Hispanic or Latino','Ethnicity','Ethnicity','Ethnicity','S','Hispanic');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','DEMOGRAPHIC','N','2186-5',38003564,'Not Hispanic or Latino','Ethnicity','Ethnicity','Ethnicity','S','Not Hispanic');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','DEMOGRAPHIC','NI','No Information',0,'No Information','Ethnicity','Ethnicity','Ethnicity','S','NI');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','DEMOGRAPHIC','OT','Other',0,'Other','Ethnicity','Ethnicity','Ethnicity','S','OT');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','DEMOGRAPHIC','R','Refuse to answer',0,'Refuse to answer','Ethnicity','Ethnicity','Ethnicity','S','R');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','DEMOGRAPHIC','UN','Unknown',0,'Unknown','Ethnicity','Ethnicity','Ethnicity','S','UN');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('I2B2ACT','PATIENT_DIMENSION','DEM|HISP:Y','2135-2',38003563,'Hispanic or Latino','Ethnicity','Ethnicity','Ethnicity','S','Hispanic');
Insert into CDMH_STAGING.ETHNICITY_XWALK (CDM_NAME,CDM_TBL,SRC_ETHNICITY,FHIR_CD,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('I2B2ACT','PATIENT_DIMENSION','DEM|HISP:N','2186-5',38003564,'Not Hispanic or Latino','Ethnicity','Ethnicity','Ethnicity','S','Not Hispanic');