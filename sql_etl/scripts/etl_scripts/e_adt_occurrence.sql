CREATE SEQUENCE if not exists SITE_pedsnet.adt_seq;

Begin;
insert into SITE_pedsnet.adt_occurrence (
    adt_occurrence_id,
    person_id,
    visit_occurrence_id,

)

select 
    nextval('SITE_pedsnet.adt_seq')::bigint as adt_occurrence_id,
    person.person_id AS person_id,   
    enc.encounterid as visit_occurrence_id,
    coalesce(OBSGEN_START_DATE, '0000-01-01') as adt_date,
    coalesce(OBSGEN_START_DATE, '0000-01-01')::timestamp as adt_datetime,
    enc.facilityid as care_site_id,
    2000000078 as service_concept_id, --PICU
    2000000083 as adt_type_concept_id, --admission
    'obs_gen ' || OBSGEN_TYPE || OBSGEN_CODE as service_source_value
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
    and RESULT_TEXT <> 'N';