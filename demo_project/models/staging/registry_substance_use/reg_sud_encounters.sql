select
    pat_id,
    hsp_account_id,
    pat_enc_csn_id,

    cast(contact_date as date) as contact_date,

    year,
    enc_type_nm,
    department_name,
    department_specialty,
    appt_time,
    appt_status_nm,
    rsn_for_visit_list,
    other_rsn_list,
    adt_arrival_time,
    hosp_admsn_time,
    inp_adm_date,
    op_adm_date,
    emer_adm_date,
    hosp_disch_time,
    tot_chgs,
    visit_prov_id,
    pcp_prov_id,
    admission_prov_id,
    bill_attend_prov_id,

    dayname(contact_date) as day_of_week,

    date_diff('minute', hosp_admsn_time, hosp_disch_time) as hosp_los_mins,

    date_diff('hour', hosp_admsn_time, hosp_disch_time) as hosp_los_hrs,

    date_diff('day', hosp_admsn_time, hosp_disch_time) as hosp_los_days,

    case
        when
            adt_arrival_time is not null
            and hosp_admsn_time is null
            then 1
    end as left_before_roomed_flag,

    -- "face-to-face" definition comes from emr reporting
    -- sbirt added by mcvitano01@jpshealth.org
    case
        when
            enc_type_nm in (
                'Initial consult',
                'Anti-coag visit',
                'Procedure visit',
                'Office Visit',
                'Routine Prenatal',
                'Initial Prenatal',
                'Postpartum Visit',
                'Walk-In',
                'Nurse Only',
                'Social Work',
                'Surgical Consult',
                'Clinical Support',
                'Pre-OP Evaluation',
                'Hospital Encounter',
                'Appointment',
                'TH-Phone',
                'TH-Video',
                'Telemedicine',
                'SBIRT'
            )
            then 1
    end as face_to_face_flag,

    case
        when
            enc_type_nm in (
                'TH-Phone',
                'TH-Video',
                'Telemedicine'
            )
            then 1
    end as telehealth_flag,

    -- PCMH list needs to be validated
    -- some clinics (e.g., Legacy Health) no longer exist
    -- some excluded clinics may qualify as a PCMH (e.g., Welcome Clinic)
    -- some included clinics may not qualify (e.g., Southeast General Surgery)
    case
        when department_name like '%diamond hill%'
            then 'diamond hill'
        when department_name like '%family health%'
            then 'family health'
        when department_name like '%legacy health%'
            then 'legacy health'
        when department_name like '%magnolia%'
            then 'magnolia'
        when department_name like '%northeast%'
            then 'northeast'
        when department_name like '%northwest%'
            then 'northwest'
        when department_name like '%polytechnic%'
            then 'polytechnic'
        when department_name like '%south campus%'
            then 'south campus'
        when department_name like '%southeast%'
            then 'southeast'
        when department_name like '%stop six%'
            then 'stop six'
        when department_name like '%true worth%'
            then 'true worth'
        when department_name like '%viola m pitts%'
            then 'viola pitts'
        when department_name like '%watauga%'
            then 'watauga'
    end as pcmh_parent,

    case
        when product_type_nm in ('COMMERCIAL', 'NON-CONTRACTED COMMERCIAL')
            then 'commercial'
        when product_type_nm in ('MEDICARE', 'MANAGED MEDICARE')
            then 'medicare'
        when product_type_nm in ('MEDICAID', 'MANAGED MEDICAID')
            then 'medicaid'
        when product_type_nm in ('CHARITY')
            then 'hospital-based medical assistance'
        when product_type_nm in ('GRANTS')
            then 'grants'
        when product_type_nm in ('GOVERNMENT OTHER')
            then 'other'
        -- escape single-quote (') or it will throw off dbt's compilation step
        when product_type_nm in ('LIABILITY', 'WORKER''S COMP')
            then 'liability'
        when
            product_type_nm in ('TARRANT COUNTY INMATE')
            or payor_name like '%jail%'
            or payor_name like '%inmate%'
            then 'inmate'
        when product_type_nm like '%self%'
            then 'self-pay'
        else 'unknown'
    end as insurance_class

from {{ source('Substance Use Disorder Registry', 'encounters') }}
