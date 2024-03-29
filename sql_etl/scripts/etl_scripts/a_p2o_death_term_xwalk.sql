CREATE TABLE CDMH_STAGING.P2O_DEATH_TERM_XWALK
   (CDM_SOURCE character varying(20),
	CDM_TBL character varying(20),
	CDM_COLUMN_NAME character varying(50),
	SRC_CODE character varying(20),
	SRC_CD_DESCRIPTION character varying(120),
	TARGET_CONCEPT_ID numeric(32,0),
	TARGET_CONCEPT_NAME character varying(250),
	TARGET_DOMAIN_ID character varying(20),
	TARGET_VOCABULARY_ID character varying(30),
	TARGET_CONCEPT_CLASS_ID character varying(20),
	TARGET_STANDARD_CONCEPT character varying(1),
	TARGET_CONCEPT_CODE character varying(50)
   ) ;

Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','B','B=Both  month  and  day  imputed',42530742,'(death_date_imputed_42528380) Both month and day imputed (concept_id = 42530742)','Meas Value','LOINC','Answer','S','LA26905-2');

Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','D','Day  imputed',42530783,'(death_date_imputed_42528380) Day imputed (concept_id = 42530783)','Meas Value','LOINC','Answer','S','LA26904-5');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','M','Month  imputed',42531065,'(death_date_imputed_42528380) Month imputed (concept_id = 42531065)','Meas Value','LOINC','Answer','S','LA26906-0');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','N','Not  imputed',42531037,'(death_date_imputed_42528380) Not imputed (concept_id = 42531037)','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','NI','No  information',46237210,'(death_date_imputed_42528380) No information (concept_id = 46237210)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','OT','Other',45878142,'(death_date_imputed_42528380) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_DATE_IMPUTE','UN','Unknown',45877986,'(death_date_imputed_42528380) Unknown (concept_id = 45877986)','Meas Value','LOINC','Answer','S','OMOP4822220');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','D','Social  Security',32519,'(death_type) US Social Security Death Master File record (concept_id = 32519)','Type Concept','Death Type','Death Type','S','LA46-8');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','L','Other,  locally  defined',45878142,'(other_ni_unk) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','OMOP4822221');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','N','National  Death  Index',32518,'(death_type) Other government reported or identified death (concept_id = 32518)','Type Concept','Death Type','Death Type','S','GAP');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','DR','Derived',0,'GAP','GAP','GAP','GAP',null,'LA21413-2');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','NI','No  information',46237210,'(other_ni_unk) No information (concept_id = 46237210)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','OT','Other',45878142,'(other_ni_unk) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','OMOP4822221');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','S','State  Death  files',32518,'(death_type) Other government reported or identified death (concept_id = 32518)','Type Concept','Death Type','Death Type','S','GAP');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','T','Tumor  data',0,'GAP','GAP','GAP','GAP',null,'LA4489-6');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH','DEATH_SOURCE','UN','Unknown',45877986,'(other_ni_unk) Unknown (concept_id = 45877986)','Meas Value','LOINC','Answer','S','LA9206-9');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_CONFIDENCE','E','Excellent',45881924,'(confidence_in_death_cause_42528382) Excellent (concept_id = 45881924)','Meas Value','LOINC','Answer','S','LA8968-5');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_CONFIDENCE','F','Fair',45876387,'(confidence_in_death_cause_42528382) Fair (concept_id = 45876387)','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_CONFIDENCE','NI','No  information',46237210,'(confidence_in_death_cause_42528382) No information (concept_id = 46237210)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_CONFIDENCE','OT','OT=Other',45878142,'(confidence_in_death_cause_42528382) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA8969-3');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_CONFIDENCE','P','P=Poor',45876751,'(confidence_in_death_cause_42528382) Poor (concept_id = 45876751)','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_CONFIDENCE','UN','UN=Unknown',45877986,'(confidence_in_death_cause_42528382) Unknown (concept_id = 45877986)','Meas Value','LOINC','Answer','S','LA26680-1');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','C','C=Contributory',36309756,'(obs_cause_of_death_seq_42528938) Contributory (concept_id = 36309756)','Meas Value','LOINC','Answer','S','LA26682-7');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','I','I=Immediate/Primary',36310205,'(obs_cause_of_death_seq_42528938) Immediate/Primary (concept_id = 36310205)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','O','O=Other',45878142,'(obs_cause_of_death_seq_42528938) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA26681-9');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','U','U=Underlying',36310984,'(obs_cause_of_death_seq_42528938) Underlying (concept_id = 36310984)','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','NI','NI=No  information',46237210,'(obs_cause_of_death_seq_42528938) No information (concept_id = 46237210)','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','UN','UN=Unknown',45877986,'(obs_cause_of_death_seq_42528938) Unknown (concept_id = 45877986)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_DEATH_TERM_XWALK
 (CDM_SOURCE,CDM_TBL,CDM_COLUMN_NAME,SRC_CODE,SRC_CD_DESCRIPTION,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE)
 values ('PCORnet','DEATH_CAUSE','DEATH_CAUSE_TYPE','OT','OT=Other',45878142,'(obs_cause_of_death_seq_42528938) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S',null);
--------------------------------------------------------
--  Constraints for Table P2O_DEATH_TERM_XWALK
--------------------------------------------------------

  ALTER TABLE CDMH_STAGING.P2O_DEATH_TERM_XWALK alter column SRC_CODE SET NOT NULL;
  ALTER TABLE CDMH_STAGING.P2O_DEATH_TERM_XWALK alter column CDM_COLUMN_NAME SET NOT NULL;
  ALTER TABLE CDMH_STAGING.P2O_DEATH_TERM_XWALK alter column CDM_TBL SET NOT NULL;
  ALTER TABLE CDMH_STAGING.P2O_DEATH_TERM_XWALK alter column CDM_SOURCE SET NOT NULL;