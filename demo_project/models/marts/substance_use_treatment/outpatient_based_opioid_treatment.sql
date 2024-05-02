{{ config(materialized='table') }}

with

obot_appts as (
    select
        pat_id,
        pat_enc_csn_id,
        day_of_week,
        year,
        appt_time,
        appt_status_nm,
        rsn_for_visit_list,
        other_rsn_list,
        insurance_class

    from {{ ref('reg_sud_encounters') }}

    where department_name = 'JPS START'

),

ed_encs as (

    select
        pat_id,
        pat_enc_csn_id,
        emer_adm_date,
        hosp_disch_time as emer_disch_time,
        rsn_for_visit_list as emer_rsn_visit,
        other_rsn_list as emer_rsn_other,

        case
            when inp_adm_date is not null then 'y'
            else 'n'
        end as emer_hospitalized_yn

    from {{ ref('reg_sud_encounters') }}
    where
        emer_adm_date is not null
        or department_name = 'JPS EMERGENCY'
        or department_name = 'JPS URGENT CARE CENTER MAIN'

),

bridge_enrollment as (

    select
        pat_id,
        enroll_status_name as bridge_status,
        enroll_start_dt as bridge_start_date,
        enroll_end_dt as bridge_end_date

    from {{ source('Substance Use Disorder Registry', 'bridge_enrollment') }}

),

bup_orders as (

    select
        pat_id,
        order_inst as bup_previous_time

    from {{ ref('reg_sud_medication_orders') }}

    where atc_code in ('N07BC51', 'N07BC01')

),

demographics as (

    select
        pat_id,
        age_yrs,
        sex,
        raceth,
        language_primary,
        relationship_status

    from {{ ref('reg_sud_patients') }}

),

merged_one_to_many as (

    select
        obot_appts.pat_id,
        obot_appts.pat_enc_csn_id,
        obot_appts.day_of_week,
        obot_appts.year,
        obot_appts.appt_time,
        obot_appts.appt_status_nm,
        obot_appts.rsn_for_visit_list,
        obot_appts.other_rsn_list,
        obot_appts.insurance_class,
        bup_orders.bup_previous_time,
        ed_encs.emer_adm_date,
        ed_encs.emer_hospitalized_yn,
        ed_encs.emer_disch_time,
        ed_encs.emer_rsn_visit,
        ed_encs.emer_rsn_other,
        bridge_enrollment.bridge_status,
        bridge_enrollment.bridge_start_date,
        bridge_enrollment.bridge_end_date,
        demographics.age_yrs,
        demographics.sex,
        demographics.raceth,
        demographics.language_primary,
        demographics.relationship_status,
        row_number() over (
            partition by obot_appts.pat_enc_csn_id
            order by ed_encs.emer_adm_date desc
        ) as obot_appts_ed_seq

    from obot_appts

    left join ed_encs
        on obot_appts.pat_id = ed_encs.pat_id

    left join bridge_enrollment
        on obot_appts.pat_id = bridge_enrollment.pat_id

    left join bup_orders
        on obot_appts.pat_id = bup_orders.pat_id

    left join demographics
        on obot_appts.pat_id = demographics.pat_id

    where
        ed_encs.emer_adm_date < obot_appts.appt_time
        and bup_orders.bup_previous_time < obot_appts.appt_time

),

-- keep single (previous) ED visit per-obot encounter (in case multiple)
obot_appts_joined_single_prior_ed as (

    select *

    from merged_one_to_many

    where obot_appts_ed_seq = 1
)

select * from obot_appts_joined_single_prior_ed
