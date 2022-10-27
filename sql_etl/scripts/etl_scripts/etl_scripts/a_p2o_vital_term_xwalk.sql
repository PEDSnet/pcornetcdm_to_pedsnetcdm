CREATE TABLE CDMH_STAGING.P2O_VITAL_TERM_XWALK 
(
  SRC_CDM character varying(20) 
, SRC_CDM_TBL character varying(30) 
, SRC_CDM_COLUMN character varying(50) 
, SRC_CODE character varying(20) 
, SRC_CODE_DESCRIP character varying(250) 
, TARGET_CONCEPT_ID integer 
, TARGET_CONCEPT_NAME character varying(200) 
, TARGET_DOMAIN_ID character varying(20) 
, TARGET_VOCABULARY_ID character varying(30) 
, TARGET_CONCEPT_CLASS_ID character varying(20) 
, TARGET_STANDARD_CONCEPT character varying(2) 
, TARGET_CONCEPT_CODE character varying(50) 
) ;


Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','DIASTOLIC_BP_POSITION','01','01=Sitting',3034703,'Diastolic blood pressure--sitting','Measurement','SNOMED','Observable Entity','S','163035008');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','DIASTOLIC_BP_POSITION','02','02=Standing',3019962,'Diastolic blood pressure--standing','Measurement','SNOMED','Observable Entity','S','163034007');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','DIASTOLIC_BP_POSITION','03','03=Supine',36304130,'Diastolic blood pressure-lying L-lateral position','Measurement','SNOMED','Observable Entity','S','163033001');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','DIASTOLIC_BP_POSITION','NI','NI=No  information',4154790,'Diastolic blood pressure','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','DIASTOLIC_BP_POSITION','OT','OT=Other',4154790,'(other_ni_unk) Other','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','DIASTOLIC_BP_POSITION','UN','UN=Unknown',4154790,'(other_ni_unk) Unknown','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SYSTOLIC_BP_POSITION','01','01=Sitting',3018586,'Systolic blood pressure--sitting','Measurement','SNOMED','Observable Entity','S','163035008');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SYSTOLIC_BP_POSITION','02','02=Standing',3035856,'Systolic blood pressure--standing','Measurement','SNOMED','Observable Entity','S','163034007');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SYSTOLIC_BP_POSITION','03','03=Supine',36303812,'Systolic blood pressure-lying L-lateral position','Measurement','SNOMED','Observable Entity','S','163033001');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SYSTOLIC_BP_POSITION','NI','NI=No  information',4152194,'Systolic blood pressure','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SYSTOLIC_BP_POSITION','OT','OT=Other',4152194,'(other_ni_unk) Other','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SYSTOLIC_BP_POSITION','UN','UN=Unknown',4152194,'(other_ni_unk) Unknown','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','01','01=Current  every  day  smoker',45881517,'(obs_smoking_status_43054909) Current every day smoker (concept_id = 45881517)','Meas Value','LOINC','Answer','S','LA18976-3');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','02','02=Current  some  day  smoker',45884037,'(obs_smoking_status_43054909) Current some day smoker (concept_id = 45884037)','Meas Value','LOINC','Answer','S','LA18977-1');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','03','03=Former  smoker',45883458,'(obs_smoking_status_43054909) Former smoker (concept_id = 45883458)','Meas Value','LOINC','Answer','S','LA15920-4');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','04','04=Never  smoker',45879404,'(obs_smoking_status_43054909) Never smoker (concept_id = 45879404)','Meas Value','LOINC','Answer','S','LA18978-9');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','05','05=Smoker,  current  status  unknown',45881518,'(obs_smoking_status_43054909) Smoker, current status unknown (concept_id = 45881518)','Meas Value','LOINC','Answer','S','LA18979-7');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','06','06=Unknown  if  ever  smoked',45885135,'(obs_smoking_status_43054909) Unknown if ever smoked (concept_id = 45885135)','Meas Value','LOINC','Answer','S','LA18980-5');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','07','07=Heavy  tobacco  smoker',45884038,'(obs_smoking_status_43054909) Heavy tobacco smoker (concept_id = 45884038)','Meas Value','LOINC','Answer','S','LA18981-3');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','08','08=Light  tobacco  smoker',45878118,'(obs_smoking_status_43054909) Light tobacco smoker (concept_id = 45878118)','Meas Value','LOINC','Answer','S','LA18982-1');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','NI','NI=No  information',46237210,'(other_ni_unk) No information (concept_id = 46237210)','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','OT','OT=Other',45878142,'(other_ni_unk) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','SMOKING','UN','UN=Unknown',45878142,'(other_ni_unk) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','01','01=Current  user',36307579,'(obs_tobacco_use_36305168) Current some day user (concept_id = 36307579)','Meas Value','LOINC','Answer','S','LA28392-1');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','02','02=Never',36308879,'(obs_tobacco_use_36305168) Never used (concept_id = 36308879)','Meas Value','LOINC','Answer','S','LA4519-0');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','03','03=Quit/former  user',36307819,'(obs_tobacco_use_36305168) Former user (concept_id = 36307819)','Meas Value','LOINC','Answer','S','LA28393-9');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','04','04=Passive  or  environmental  exposure',37017812,'(obs_tobacco_use_36305168) Environmental tobacco smoke exposure (concept_id = 37017812)','Observation','SNOMED','Observable Entity','S','714151003');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','06','06=Not  asked',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','NI','NI=No  information',46237210,'(other_ni_unk) No information (concept_id = 46237210)','Meas Value','LOINC','Answer','S','LA21413-2');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','OT','OT=Other',45877986,'(other_ni_unk) Unknown (concept_id = 45877986)','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO','UN','UN=Unknown',45877986,'(obs_tobacco_use_36305168) Unknown (concept_id = 45877986)','Meas Value','LOINC','Answer','S','LA4489-6');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','01','01=Smoked  tobacco  only',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','02','02=Non-smoked  tobacco  only',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','03','03=Use  of  both  smoked  and  non-smoked  tobacco  products',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','04','04=None',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','05','05=Use  of  smoked  tobacco  but  no  information  about  non-smoked  tobacco  use',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','NI','NI=No  information',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','OT','OT=Other',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','TOBACCO_TYPE','UN','UN=Unknown',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','HC','HC=Healthcare  delivery  setting',32489,'(measurement_type) Accelerated lab result (concept_id = 32489)','Type Concept','Meas Type','Meas Type','S','OMOP4822268');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','HD','HD=Healthcare  device  direct  feed',32489,'(measurement_type) Accelerated lab result (concept_id = 32489)','Type Concept','Meas Type','Meas Type','S','OMOP4822268');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','DR','DR=Derived',45754907,'(measurement_type) Derived value (concept_id = 45754907)','Type Concept','Meas Type','Meas Type','S','OMOP4822276');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','NI','NI=No  information',46237210,'(other_ni_unk) No information (concept_id = 46237210)','Type Concept','Meas Type','Meas Type','S','OMOP4822272');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','OT','OT=Other',45878142,'(other_ni_unk) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA46-8');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','PD','PD=Patient  device  direct  feed',0,'Gap','-','-','-','-','-');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','PR','PR=Patient-reported',44818704,'(measurement_type) Patient reported value (concept_id = 44818704)','Type Concept','Meas Type','Meas Type','S','OMOP4822272');
Insert into CDMH_STAGING.P2O_VITAL_TERM_XWALK (SRC_CDM,SRC_CDM_TBL,SRC_CDM_COLUMN,SRC_CODE,SRC_CODE_DESCRIP,TARGET_CONCEPT_ID,TARGET_CONCEPT_NAME,TARGET_DOMAIN_ID,TARGET_VOCABULARY_ID,TARGET_CONCEPT_CLASS_ID,TARGET_STANDARD_CONCEPT,TARGET_CONCEPT_CODE) values ('PCORnet','VITAL','VITAL_SOURCE','UN','UN=Unknown',45878142,'(other_ni_unk) Other (concept_id = 45878142)','Meas Value','LOINC','Answer','S','LA46-8');
