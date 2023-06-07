
create sequence if not exists SITE_pedsnet.device_seq;

-- add ventilator from procedures
begin;
INSERT INTO SITE_pedsnet.device_exposure (
    device_concept_id, 
    device_exposure_id,
	device_exposure_start_date, 
    device_exposure_start_datetime,
	device_source_concept_id,
	device_source_value, 
	device_type_concept_id, 
    placement_concept_id
	person_id, 
	provider_id, 
	visit_occurrence_id
)

select 
    case
        when px = '96.7' then 2008006
        when px = '96.7' then 2008007
        when px = '96.71' then 2008008
        when px = '96.72' then 2008009
        when px = '94002' then 2314000
        when px = '94003' then 2314001
        when px = '94004' then 2314002
        when px = '5A19054' then 2788035
        when px = '5A1935Z' then 2788036
        when px = '5A1945Z' then 2788037
        when px = '5A1955Z' then 2788038
        when px = '5A1945' then 2800859
        when px = '5A0935' then 2805870
        when px = '5A0955' then 2813710
        when px = '5A1955' then 2834015
        when px = '5A0945' then 2867784
        when px = '5A1905' then 2886050
        when px = '5A1935' then 2893766
        when px = '733496000' then 37116689
        when px = '243141005' then 4055261
        when px = '243150007' then 4055375
        when px = '243153009' then 4055376
        when px = '243154003' then 4055377
        when px = '182687005' then 4057263
        when px = '243148004' then 4072503
        when px = '243149007' then 4072504
        when px = '243151006' then 4072505
        when px = '243143008' then 4072515
        when px = '243152004' then 4072516
        when px = '243156001' then 4072517
        when px = '243174005' then 4072633
        when px = '243147009' then 4074665
        when px = '243155002' then 4074666
        when px = '286813003' then 4113618
        when px = '286812008' then 4120570
        when px = '397998001' then 4134853
        when px = '425447009' then 4139542
        when px = '266690007' then 4145646
        when px = '266700009' then 4145647
        when px = '45851008' then 4164571
        when px = '424172009' then 4174085
        when px = '55089006' then 4208272
        when px = '8948006' then 4229907
        when px = '40617009' then 4230167
        when px = '405609003' then 4236738
        when px = '408853006' then 4237460
        when px = '59427005' then 4245036
        when px = '408852001' then 4254209
        when px = '94656' then 42738852
        when px = '94657' then 42738853
        when px = '397899008' then 4287922
        when px = '424282004' then 4312631
        when px = '243181003' then 4347666
        when px = '243182005' then 4347667
        when px = '243184006' then 4347913
        when px = '226000000000000' then 44790095
        when px = '232000000000000' then 44791135
        else 0
    end as device_concept_id,
    nextval('SITE_pedsnet.device_seq') as device_exposure_id,
    case 
        when proc.px_date is not null and SITE_pedsnet.is_date(proc.px_date::varchar) then proc.px_date::date
        when proc.admit_date is not null and SITE_pedsnet.is_date(proc.admit_date::varchar) then proc.admit_date::date
        else '0001-01-01'::date
    end as device_exposure_start_date,
    case
        when proc.px_date is null then '0001-01-01'::timestamp
        when SITE_pedsnet.is_date(proc.px_date::varchar) then proc.px_date::timestamp
        else '0001-01-01'::timestamp
    end as device_exposure_start_datetime,
    0 as device_source_concept_id,
    px as device_source_value,
    44818707 as device_type_concept_id,
    44814650 as placement_concept_id,
    person.patid as person_id,
    enc.providerid as provider_id,
    enc.encounterid as visit_occurrence_id
from 
    SITE_pcornet.procedures proc
inner join 
	SITE_pedsnet.person person 
    on proc.patid = person.person_source_value
left join
    SITE_pcornet.encounter enc
    on enc.encounterid = proc.encounterid
where px in 
    (
    '96.7',
    '96.7',
    '96.71',
    '96.72',
    '94002',
    '94003',
    '94004',
    '5A19054',
    '5A1935Z',
    '5A1945Z',
    '5A1955Z',
    '5A1945',
    '5A0935',
    '5A0955',
    '5A1955',
    '5A0945',
    '5A1905',
    '5A1935',
    '733496000',
    '243141005',
    '243150007',
    '243153009',
    '243154003',
    '182687005',
    '243148004',
    '243149007',
    '243151006',
    '243143008',
    '243152004',
    '243156001',
    '243174005',
    '243147009',
    '243155002',
    '286813003',
    '286812008',
    '397998001',
    '425447009',
    '266690007',
    '266700009',
    '45851008',
    '424172009',
    '55089006',
    '8948006',
    '40617009',
    '405609003',
    '408853006',
    '59427005',
    '408852001',
    '94656',
    '94657',
    '397899008',
    '424282004',
    '243181003',
    '243182005',
    '243184006',
    '226000000000000',
    '232000000000000'
    );
commit;

-- PC_COVID 3000 ventilator records from obs_gen
begin;
INSERT INTO SITE_pedsnet.device_exposure (
    device_concept_id, 
    device_exposure_id,
	device_exposure_start_date, 
    device_exposure_start_datetime,
	device_source_concept_id,
	device_source_value, 
	device_type_concept_id, 
    placement_concept_id,
	person_id, 
	provider_id, 
	visit_occurrence_id
) 
select 
    44791135 as device_concept_id,
    nextval('SITE_pedsnet.device_seq') as device_exposure_id,
    case 
        when og.OBSGEN_START_DATE is not null and SITE_pedsnet.is_date(og.OBSGEN_START_DATE::varchar) then og.OBSGEN_START_DATE::date
        when og.OBSGEN_START_DATE is not null and SITE_pedsnet.is_date(og.OBSGEN_START_DATE::varchar) then og.OBSGEN_START_DATE::date
        else '0001-01-01'::date
    end as device_exposure_start_date,
    case
        when og.OBSGEN_START_DATE is null then '0001-01-01'::timestamp
        when SITE_pedsnet.is_date(og.OBSGEN_START_DATE::varchar) then og.OBSGEN_START_DATE::timestamp
        else '0001-01-01'::timestamp
    end as device_exposure_start_datetime,
    0 as device_source_concept_id,
    OBSGEN_TYPE || OBSGEN_CODE  as device_source_value,
    44818707 as device_type_concept_id,
    44814650 as placement_concept_id,
    og.patid as person_id,
    enc.providerid as provider_id,
    enc.encounterid as visit_occurrence_id
from 
    SITE_pcornet.obs_gen og 
inner join 
	SITE_pedsnet.person person 
    on og.patid = person.person_source_value
left join
    SITE_pcornet.encounter enc
    on enc.encounterid = og.encounterid
where
    OBSGEN_TYPE  = 'PC_COVID' 
    and OBSGEN_CODE = '3000';
commit;