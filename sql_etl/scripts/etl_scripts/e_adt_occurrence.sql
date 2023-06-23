CREATE SEQUENCE if not exists SITE_pedsnet.adt_seq;

-- insert obs_gen covid ICU admissions into adt_occurrence
begin;
insert into SITE_pedsnet.adt_occurrence (
    adt_occurrence_id,
    person_id,
    visit_occurrence_id,
    adt_date,
    adt_datetime,
    care_site_id,
    service_concept_id,
    adt_type_concept_id,
    service_source_value
)
select 
    nextval('SITE_pedsnet.adt_seq')::bigint as adt_occurrence_id,
    person.person_id AS person_id,   
    enc.encounterid as visit_occurrence_id,
    case 
        when og.OBSGEN_START_DATE is not null and SITE_pedsnet.is_date(og.OBSGEN_START_DATE::varchar) then og.OBSGEN_START_DATE::date
        when og.OBSGEN_START_DATE is not null and SITE_pedsnet.is_date(og.OBSGEN_START_DATE::varchar) then og.OBSGEN_START_DATE::date
        else '0001-01-01'::date
    end as adt_date,
    case
        when og.OBSGEN_START_DATE is null then '0001-01-01'::timestamp
        when SITE_pedsnet.is_date(og.OBSGEN_START_DATE::varchar) then og.OBSGEN_START_DATE::timestamp
        else '0001-01-01'::timestamp
    end as adt_datetime,
    enc.facilityid as care_site_id,
    2000000078 as service_concept_id, --PICU
    2000000083 as adt_type_concept_id, --admission
    'obs_gen ' || OBSGEN_TYPE || ' ' || OBSGEN_CODE as service_source_value
from 
    SITE_pcornet.obs_gen og
inner join 
    SITE_pedsnet.person person 
    on og.patid=person.person_source_value
left join 
    SITE_pcornet.encounter enc
    on og.encounterid=enc.encounterid
where 
    OBSGEN_TYPE= 'PC_COVID'
    and OBSGEN_CODE = '2000'
    and OBSGEN_RESULT_TEXT <> 'N';
commit;

-- insert ICU submissions that are procedures
begin;
insert into SITE_pedsnet.adt_occurrence (
    adt_occurrence_id,
    person_id,
    visit_occurrence_id,
    adt_date,
    adt_datetime,
    care_site_id,
    service_concept_id,
    adt_type_concept_id,
    service_source_value
)
select
    nextval('SITE_pedsnet.adt_seq')::bigint as adt_occurrence_id,
    person.person_id AS person_id,   
    enc.encounterid as visit_occurrence_id,
    case 
        when proc.px_date is not null and SITE_pedsnet.is_date(proc.px_date::varchar) then proc.px_date::date
        when proc.admit_date is not null and SITE_pedsnet.is_date(proc.admit_date::varchar) then proc.admit_date::date
        else '0001-01-01'::date
    end as adt_date,
    case
        when proc.px_date is null then '0001-01-01'::timestamp
        when SITE_pedsnet.is_date(proc.px_date::varchar) then proc.px_date::timestamp
        else '0001-01-01'::timestamp
    end as adt_datetime,
    enc.facilityid as care_site_id,
    2000000078 as service_concept_id, --PICU
    2000000083 as adt_type_concept_id, --admission
    'procedures ' || px || ' ' || raw_px as service_source_value
from 
    SITE_pcornet.procedures proc 
inner join 
    SITE_pedsnet.person person 
    on proc.patid=person.person_source_value
left join 
    SITE_pcornet.encounter enc
    on proc.encounterid=enc.encounterid
where 
    px in (
        '99291', -- ICU
        '99292', -- ICU
        '99293', -- PICU
        '99294' -- PICU
        )

