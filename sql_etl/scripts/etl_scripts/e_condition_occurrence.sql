
CREATE SEQUENCE if not exists SITE_pedsnet.cond_occ_seq;

create or replace function SITE_pedsnet.is_date(s varchar) returns boolean as $$
begin
          perform s::date;
          return true;
        exception when others then
                  return false;
end;
$$ language plpgsql;

begin;
-- problem_list
INSERT INTO SITE_pedsnet.condition_occurrence(
        condition_concept_id,
        condition_source_concept_id,
        condition_source_value,
        condition_end_date,
        condition_end_datetime,
        condition_occurrence_id,
        condition_start_date,
        condition_start_datetime, 
        condition_status_concept_id,
        condition_status_source_value,
        condition_type_concept_id,
        person_id,
        poa_concept_id, 
        provider_id,
        stop_reason,
        visit_occurrence_id)
SELECT distinct
    coalesce(
        case
            --covid diagnosis codes
            when cond.condition = '398447004' then 320651::int
            when cond.condition = '713084008' then 37016927::int
            when cond.condition = '1240521000000100' then 37310254::int
            when cond.condition = '1240441000000100' then 37310260::int
            when cond.condition = '1240571000000100' then 37310283::int
            when cond.condition = '1240561000000100' then 37310284::int
            when cond.condition = '1240551000000100' then 37310285::int
            when cond.condition = '1240541000000100' then 37310286::int
            when cond.condition = '1240531000000100' then 37310287::int
            when cond.condition = '840546002' then 37311059::int
            when cond.condition = '840539006' then 37311061::int
            when cond.condition = '441590008' then 40479642::int
            when cond.condition = '444482005' then 40479782::int
            when cond.condition = '408688009' then 4248811::int
            when cond.condition = '186747009' then 439676::int
            when cond.condition = '702547000' then 45765578::int
            when cond.condition = 'OMOP4873948' then 756022::int
            when cond.condition = 'OMOP4873906' then 756023::int
            when cond.condition = 'OMOP4873964' then 756027::int
            when cond.condition = 'OMOP4873909' then 756031::int
            when cond.condition = 'OMOP4873949' then 756035::int
            when cond.condition = 'OMOP4873953' then 756037::int
            when cond.condition = 'OMOP4873907' then 756039::int
            when cond.condition = 'OMOP4873957' then 756041::int
            when cond.condition = 'OMOP4873911' then 756044::int
            when cond.condition = 'OMOP4873954' then 756045::int
            when cond.condition = 'OMOP4873958' then 756047::int
            when cond.condition = 'OMOP4873947' then 756050::int
            when cond.condition = 'OMOP4873910' then 756061::int
            when cond.condition = 'OMOP4873951' then 756062::int
            when cond.condition = 'OMOP4873946' then 756064::int
            when cond.condition = 'OMOP4873950' then 756068::int
            when cond.condition = 'OMOP4873959' then 756069::int
            when cond.condition = 'OMOP4873956' then 756072::int
            when cond.condition = 'OMOP4873952' then 756075::int
            when cond.condition = 'OMOP4873961' then 756076::int
            when cond.condition = 'OMOP4873960' then 756077::int
            when cond.condition = 'OMOP4873955' then 756079::int
            when cond.condition = 'OMOP4873908' then 756081::int
            when cond.condition = 'OMOP4873963' then 756082::int
            when cond.condition = 'OMOP4873962' then 756083::int
            when cond.condition = '840544004' then 37311060::int
            when cond.condition = '840533007' then 37311065::int
            when cond.condition = 'OMOP4912978' then 704996::int
            when cond.condition = '138389411000119000' then 3661405::int
            when cond.condition = '674814021000119000' then 3661406::int
            when cond.condition = '882784691000119000' then 3661408::int
            when cond.condition = '189486241000119000' then 3662381::int
            when cond.condition = '880529761000119000' then 3663281::int
            when cond.condition = '870588003' then 3655975::int
            when cond.condition = '870590002' then 3655976::int
            when cond.condition = '870591003' then 3655977::int
            when cond.condition = '119731000146105' then 3656667::int
            when cond.condition = '119741000146102' then 3656668::int
            when cond.condition = '119981000146107' then 3656669::int
            when cond.condition = '866152006' then 3661632::int
            when cond.condition = '870589006' then 3661748::int
            when cond.condition = '119751000146104' then 3661885::int
            when cond.condition = '292508471000119000' then 3661980::int
            when cond.condition = '870577009' then 3655973::int
            when cond.condition = '1240581000000100' then 37310282::int
            when cond.condition = '870577009' then 3655973::int
            when cond.condition = 'Z86.16' then 3661980::int
            --misc codes
            when cond.condition = 'OMOP5042964' then 703578::int
            when cond.condition = 'M35.81' then 713856::int
            when cond.condition = 'U10' then 931072::int
            when cond.condition = 'U10.9' then 931073::int
            --pasc code
            when cond.condition = 'U09.9' then 766503::int
            --derivation via condition_type and OMOP vocabulary
            when cond.condition_type='09' or cond.condition_type='ICD09' then cr_icd9.concept_id_2
            when cond.condition_type='10' or cond.condition_type='ICD10' then cr_icd10.concept_id_2
            when cond.condition_type='SM' then c_snomed.concept_id
            -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.condition ~ '^V[0-9]{2}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            when cond.condition ~ '^E[0-9]{3}.?[0-9]?$' then cr_icd9.concept_id_2
            when cond.condition ~ '^[0-9]{3}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.condition ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then cr_icd10.concept_id_2
            else NULL
        END, 
    44814650)::int as condition_concept_id,
    coalesce(
        case
            when cond.condition_type='09' then c_icd9.concept_id
            when cond.condition_type='10' then c_icd10.concept_id
            when cond.condition_type='SM' then c_snomed.concept_id
            -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.condition ~ '^V[0-9]{2}.?[0-9]{0,2}$' then c_icd9.concept_id
            when cond.condition ~ '^E[0-9]{3}.?[0-9]?$' then c_icd9.concept_id
            when cond.condition ~ '^[0-9]{3}.?[0-9]{0,2}$' then c_icd9.concept_id
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.condition ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then c_icd10.concept_id
            else NULL
        end,
    44814650)::int as condition_source_concept_id,
    left(cond.condition,248) || ' | ' || cond.condition_type as condition_source_value,
    case 
        when SITE_pedsnet.is_date(cond.resolve_date::varchar) then cond.resolve_date::date 
    end as condition_end_date,
    case 
        when SITE_pedsnet.is_date(cond.resolve_date::varchar) then cond.resolve_date::timestamp 
    end as condition_end_datetime,
    nextval('SITE_pedsnet.cond_occ_seq') as condition_occurrence_id,
    case 
        when cond.onset_date::varchar is not null and SITE_pedsnet.is_date(cond.onset_date::varchar) then cond.onset_date::date
        when cond.report_date is not null and SITE_pedsnet.is_date(cond.report_date::varchar) then cond.report_date::date
	    else '0001-01-01'::date
    end as condition_start_date,
    case 
        when cond.onset_date is not null and SITE_pedsnet.is_date(cond.onset_date::varchar) then cond.onset_date::timestamp
        when cond.report_date is not null and SITE_pedsnet.is_date(cond.report_date::varchar) then cond.report_date::timestamp
	    else '0001-01-01'::timestamp
    end as condition_start_datetime,
    4230359 AS condition_status_concept_id,
    coalesce(cond.CONDITION_STATUS,cond.raw_condition_status) AS condition_status_source_value,
    2000000089 as condition_type_concept_id,
    person.person_id AS person_id,   
    44814650 as poa_concept_id, 
    enc.providerid as provider_id,   
    NULL as stop_reason,    
    enc.encounterid as visit_occurrence_id   
FROM 
    (
        select *
        from SITE_pcornet.condition
        where condition <> 'COVID'
    ) as cond
inner join 
    SITE_pedsnet.person person 
    on cond.patid=person.person_source_value
left join 
    SITE_pcornet.encounter enc
    on cond.encounterid=enc.encounterid
left join 
    vocabulary.concept c_icd9 
    on cond.condition=c_icd9.concept_code
    and c_icd9.vocabulary_id='ICD9CM'
left join 
    vocabulary.concept c_icd10 
    on cond.condition=c_icd10.concept_code
    and c_icd10.vocabulary_id='ICD10CM'
left join 
    vocabulary.concept c_snomed 
    on cond.condition=c_snomed.concept_code
    and c_snomed.vocabulary_id='SNOMED' 
    and cond.condition_type='SM'
left join 
    vocabulary.concept_relationship cr_icd9
    on c_icd9.concept_id = cr_icd9.concept_id_1
    and cr_icd9.relationship_id='Maps to'
left join 
    vocabulary.concept_relationship cr_icd10
    on c_icd10.concept_id = cr_icd10.concept_id_1
    and cr_icd10.relationship_id='Maps to';
commit;

begin;
-- visit diagnoses
INSERT INTO SITE_pedsnet.condition_occurrence(
        condition_concept_id,
        condition_occurrence_id,
        condition_source_concept_id,
        condition_source_value,
        condition_start_date,
        condition_start_datetime, 
        condition_status_concept_id,
        condition_status_source_value,
        condition_type_concept_id,
        person_id,
        poa_concept_id, 
        provider_id,
        stop_reason,
        visit_occurrence_id)
SELECT distinct
    coalesce(
        case
            --covid diagnosis codes
            when cond.dx = '398447004' then 320651::int
            when cond.dx = '713084008' then 37016927::int
            when cond.dx = '1240521000000100' then 37310254::int
            when cond.dx = '1240441000000100' then 37310260::int
            when cond.dx = '1240571000000100' then 37310283::int
            when cond.dx = '1240561000000100' then 37310284::int
            when cond.dx = '1240551000000100' then 37310285::int
            when cond.dx = '1240541000000100' then 37310286::int
            when cond.dx = '1240531000000100' then 37310287::int
            when cond.dx = '840546002' then 37311059::int
            when cond.dx = '840539006' then 37311061::int
            when cond.dx = '441590008' then 40479642::int
            when cond.dx = '444482005' then 40479782::int
            when cond.dx = '408688009' then 4248811::int
            when cond.dx = '186747009' then 439676::int
            when cond.dx = '702547000' then 45765578::int
            when cond.dx = 'OMOP4873948' then 756022::int
            when cond.dx = 'OMOP4873906' then 756023::int
            when cond.dx = 'OMOP4873964' then 756027::int
            when cond.dx = 'OMOP4873909' then 756031::int
            when cond.dx = 'OMOP4873949' then 756035::int
            when cond.dx = 'OMOP4873953' then 756037::int
            when cond.dx = 'OMOP4873907' then 756039::int
            when cond.dx = 'OMOP4873957' then 756041::int
            when cond.dx = 'OMOP4873911' then 756044::int
            when cond.dx = 'OMOP4873954' then 756045::int
            when cond.dx = 'OMOP4873958' then 756047::int
            when cond.dx = 'OMOP4873947' then 756050::int
            when cond.dx = 'OMOP4873910' then 756061::int
            when cond.dx = 'OMOP4873951' then 756062::int
            when cond.dx = 'OMOP4873946' then 756064::int
            when cond.dx = 'OMOP4873950' then 756068::int
            when cond.dx = 'OMOP4873959' then 756069::int
            when cond.dx = 'OMOP4873956' then 756072::int
            when cond.dx = 'OMOP4873952' then 756075::int
            when cond.dx = 'OMOP4873961' then 756076::int
            when cond.dx = 'OMOP4873960' then 756077::int
            when cond.dx = 'OMOP4873955' then 756079::int
            when cond.dx = 'OMOP4873908' then 756081::int
            when cond.dx = 'OMOP4873963' then 756082::int
            when cond.dx = 'OMOP4873962' then 756083::int
            when cond.dx = '840544004' then 37311060::int
            when cond.dx = '840533007' then 37311065::int
            when cond.dx = 'OMOP4912978' then 704996::int
            when cond.dx = '138389411000119000' then 3661405::int
            when cond.dx = '674814021000119000' then 3661406::int
            when cond.dx = '882784691000119000' then 3661408::int
            when cond.dx = '189486241000119000' then 3662381::int
            when cond.dx = '880529761000119000' then 3663281::int
            when cond.dx = '870588003' then 3655975::int
            when cond.dx = '870590002' then 3655976::int
            when cond.dx = '870591003' then 3655977::int
            when cond.dx = '119731000146105' then 3656667::int
            when cond.dx = '119741000146102' then 3656668::int
            when cond.dx = '119981000146107' then 3656669::int
            when cond.dx = '866152006' then 3661632::int
            when cond.dx = '870589006' then 3661748::int
            when cond.dx = '119751000146104' then 3661885::int
            when cond.dx = '292508471000119000' then 3661980::int
            when cond.dx = '870577009' then 3655973::int
            when cond.dx = '1240581000000100' then 37310282::int
            when cond.dx = '870577009' then 3655973::int
            when cond.dx = 'Z86.16' then 3661980::int
            --misc codes
            when cond.dx = 'OMOP5042964' then 703578::int
            when cond.dx = 'M35.81' then 713856::int
            when cond.dx = 'U10' then 931072::int
            when cond.dx = 'U10.9' then 931073::int
            --pasc code
            when cond.dx = 'U09.9' then 766503::int
            --derive by dx_type
            when cond.dx_type='09' or cond.dx_type='ICD09' then cr_icd9.concept_id_2
            when cond.dx_type='10' or cond.dx_type='ICD10' then cr_icd10.concept_id_2
            when cond.dx_type='SM' then c_snomed.concept_id
             -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.dx ~ '^V[0-9]{2}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            when cond.dx ~ '^E[0-9]{3}.?[0-9]?$' then cr_icd9.concept_id_2
            when cond.dx ~ '^[0-9]{3}.?[0-9]{0,2}$' then cr_icd9.concept_id_2
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.dx ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then cr_icd10.concept_id_2
            else NULL
        end,
    44814650)::int as condition_concept_id,
    nextval('SITE_pedsnet.cond_occ_seq') as condition_occurrence_id,
    coalesce(
        case
            when cond.dx_type='09' or cond.dx_type='ICD09' then c_icd9.concept_id
            when cond.dx_type='10' or cond.dx_type='ICD10' then c_icd10.concept_id
            when cond.dx_type='SM' then c_snomed.concept_id
            -- RegEx for ICD09 codes if condition_type <>'09' 
            when cond.dx ~ '^V[0-9]{2}.?[0-9]{0,2}$' then c_icd9.concept_id
            when cond.dx ~ '^E[0-9]{3}.?[0-9]?$' then c_icd9.concept_id
            when cond.dx ~ '^[0-9]{3}.?[0-9]{0,2}$' then c_icd9.concept_id
            -- RegEx for ICD10 codes if condition_type <>'10' 
            when cond.dx ~ '^[A-Z][0-9][0-9A-Z].?[0-9A-Z]{0,4}$' then c_icd10.concept_id
            else NULL
        end,
    44814650)::int as condition_source_concept_id,
    left(cond.dx,248) || ' | ' || dx_type as condition_source_value,
    case 
        when cond.dx_date is not null and SITE_pedsnet.is_date(cond.dx_date::varchar) then cond.dx_date::date
        when cond.admit_date is not null and SITE_pedsnet.is_date(cond.admit_date::varchar) then cond.admit_date::date
        else '0001-01-01'::date
    end as condition_start_date,
    case 
        when cond.dx_date is not null and SITE_pedsnet.is_date(cond.dx_date::varchar) then cond.dx_date::timestamp
        when cond.admit_date is not null and SITE_pedsnet.is_date(cond.admit_date::varchar) then cond.admit_date::timestamp
        else '0001-01-01'::timestamp
    end as condition_start_datetime,
    4230359 AS condition_status_concept_id,
    coalesce(cond.dx_source,cond.RAW_DX_SOURCE) AS condition_status_source_value,
    case 
        when cond.enc_type='ED' and dx_origin='BI' and pdx='P'then 2000001282
        when cond.enc_type='ED' and dx_origin='OD' and pdx='P'then 2000001280
        when cond.enc_type='ED' and dx_origin='CL' and pdx='P'then 2000001281
        when cond.enc_type='ED' and dx_origin='BI' and pdx='S'then 2000001284
        when cond.enc_type='ED' and dx_origin='OD' and pdx='S'then 2000001283
        when cond.enc_type='ED' and dx_origin='CL' and pdx='S'then 2000001285
        when cond.enc_type in ('AV','OA','TH') and dx_origin='BI' and pdx='P'then 2000000096
        when cond.enc_type in ('AV','OA','TH') and dx_origin='OD' and pdx='P'then 2000000095
        when cond.enc_type in ('AV','OA','TH') and dx_origin='CL' and pdx='P'then 2000000097
        when cond.enc_type in ('AV','OA','TH') and dx_origin='BI' and pdx='S'then 2000000102
        when cond.enc_type in ('AV','OA','TH') and dx_origin='OD' and pdx='S'then 2000000101
        when cond.enc_type in ('AV','OA','TH') and dx_origin='CL' and pdx='S'then 2000000103
        when cond.enc_type in ('IP','OS','IS','EI') and dx_origin='BI' and pdx='P'then 2000000093
        when cond.enc_type in ('IP','OS','IS','EI') and dx_origin='OD' and pdx='P'then 2000000092
        when cond.enc_type in ('IP','OS','IS','EI') and dx_origin='CL' and pdx='P'then 2000000094
        when cond.enc_type in ('IP','OS','IS','EI') and dx_origin='BI' and pdx='S'then 2000000099
        when cond.enc_type in ('IP','OS','IS','EI') and dx_origin='OD' and pdx='S'then 2000000098
        when cond.enc_type in ('IP','OS','IS','EI') and dx_origin='CL' and pdx='S'then 2000000100
        else 44814650
    end as condition_type_concept_id,
    person.person_id AS person_id,   
    coalesce(
        case 
            when dx_poa='Y' then 4188539 
            else 4188540 
        end,
        44814650)::int as poa_concept_id, 
    enc.providerid as provider_id,   
    NULL as stop_reason,    
    enc.encounterid as visit_occurrence_id  
FROM 
    SITE_pcornet.diagnosis cond
inner join 
    SITE_pedsnet.person person 
    on cond.patid=person.person_source_value
left join 
    SITE_pcornet.encounter enc
    on cond.encounterid=enc.encounterid
left join 
    vocabulary.concept c_icd9 
    on cond.dx=c_icd9.concept_code
    and c_icd9.vocabulary_id='ICD9CM' 
left join 
    vocabulary.concept c_icd10 
    on cond.dx=c_icd10.concept_code
    and c_icd10.vocabulary_id='ICD10CM'
left join 
    vocabulary.concept c_snomed 
    on cond.dx=c_snomed.concept_code
    and c_snomed.vocabulary_id='SNOMED' 
    and cond.dx_type='SM'
left join 
    vocabulary.concept_relationship cr_icd9
    on c_icd9.concept_id = cr_icd9.concept_id_1
    and cr_icd9.relationship_id='Maps to'
left join 
    vocabulary.concept_relationship cr_icd10
    on c_icd10.concept_id = cr_icd10.concept_id_1
    and cr_icd10.relationship_id='Maps to';
commit;
