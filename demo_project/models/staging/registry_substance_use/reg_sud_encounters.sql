{{ config(materialized = 'table') }}

SELECT
    PAT_ID, HSP_ACCOUNT_ID, PAT_ENC_CSN_ID, 
    cast(CONTACT_DATE as date) as CONTACT_DATE, 
    dayname(CONTACT_DATE) as DAY_OF_WEEK,
    YEAR,
    ENC_TYPE_NM,
    -- "face-to-face" definition comes from EMR Reporting
    -- SBIRT added by MCvitano01@jpshealth.org
    CASE
        WHEN ENC_TYPE_NM IN (
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
            'SBIRT') THEN 1
    END AS FACE_TO_FACE_FLAG, 
    CASE
        WHEN ENC_TYPE_NM IN (
            'TH-Phone',
            'TH-Video',
            'Telemedicine') THEN 1
    END AS TELEHEALTH_FLAG, 
    DEPARTMENT_NAME, 
    DEPARTMENT_SPECIALTY,
    -- PCMH list needs to be validated
    -- Some clinics (e.g., Legacy Health) no longer exist
    -- Some excluded clinics may qualify as a PCMH (e.g., Welcome Clinic)
    -- Some included clinics may not qualify (e.g., JPS Southeast General Surgery)
    CASE
        WHEN DEPARTMENT_NAME LIKE '%DIAMOND HILL%' THEN 'Diamond Hill'
        WHEN DEPARTMENT_NAME LIKE '%FAMILY HEALTH%' THEN 'Family Health'
        WHEN DEPARTMENT_NAME LIKE '%LEGACY HEALTH%' THEN 'Legacy Health'
        WHEN DEPARTMENT_NAME LIKE '%MAGNOLIA%' THEN 'Magnolia'
        WHEN DEPARTMENT_NAME LIKE '%NORTHEAST%' THEN 'Northeast'
        WHEN DEPARTMENT_NAME LIKE '%NORTHWEST%' THEN 'Northwest'
        WHEN DEPARTMENT_NAME LIKE '%POLYTECHNIC%' THEN 'Polytechnic'
        WHEN DEPARTMENT_NAME LIKE '%SOUTH CAMPUS%' THEN 'South Campus'
        WHEN DEPARTMENT_NAME LIKE '%SOUTHEAST%' THEN 'Southeast'
        WHEN DEPARTMENT_NAME LIKE '%STOP SIX%' THEN 'Stop Six'
        WHEN DEPARTMENT_NAME LIKE '%TRUE WORTH%' THEN 'True Worth'
        WHEN DEPARTMENT_NAME LIKE '%VIOLA M PITTS%' THEN 'Viola Pitts'
        WHEN DEPARTMENT_NAME LIKE '%WATAUGA%' THEN 'Watauga'
    END AS PCMH_PARENT,
    APPT_TIME, APPT_STATUS_NM, RSN_FOR_VISIT_LIST, OTHER_RSN_LIST,
    ADT_ARRIVAL_TIME, HOSP_ADMSN_TIME, 
    INP_ADM_DATE, OP_ADM_DATE, EMER_ADM_DATE, HOSP_DISCH_TIME,
    date_diff('minute', HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS HOSP_LOS_MINS,
    date_diff('hour', HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS HOSP_LOS_HRS,
    date_diff('day', HOSP_ADMSN_TIME, HOSP_DISCH_TIME) AS HOSP_LOS_DAYS,
    CASE
        WHEN ADT_ARRIVAL_TIME IS NOT NULL
            AND HOSP_ADMSN_TIME IS NULL THEN 1
    END AS LEFT_BEFORE_ROOMED_FLAG,
    TOT_CHGS,
    VISIT_PROV_ID, PCP_PROV_ID, ADMISSION_PROV_ID, BILL_ATTEND_PROV_ID,
    CASE
        WHEN PRODUCT_TYPE_NM IN ('COMMERCIAL', 'NON-CONTRACTED COMMERCIAL') THEN 'Commercial'
        WHEN PRODUCT_TYPE_NM IN ('MEDICARE', 'MANAGED MEDICARE') THEN 'Medicare'
        WHEN PRODUCT_TYPE_NM IN ('MEDICAID', 'MANAGED MEDICAID') THEN 'Medicaid'
        WHEN PRODUCT_TYPE_NM IN ('CHARITY') THEN 'Hospital-based Medical Assistance'
        WHEN PRODUCT_TYPE_NM IN ('GRANTS') THEN 'Grants'
        WHEN PRODUCT_TYPE_NM IN ('GOVERNMENT OTHER') THEN 'Other'
        -- escape the single-quote (') or it will throw off dbt's compilation step
        WHEN PRODUCT_TYPE_NM IN ('LIABILITY', 'WORKER''S COMP') THEN 'Liability'
        WHEN PRODUCT_TYPE_NM IN ('TARRANT COUNTY INMATE') 
            OR PAYOR_NAME LIKE '%JAIL%' 
            OR PAYOR_NAME LIKE '%INMATE%' THEN 'Inmate'
        WHEN PRODUCT_TYPE_NM LIKE '%SELF%' THEN 'Self-pay'
        ELSE 'Unknown'
    END AS INSURANCE_CLASS
FROM {{ source('Substance Use Disorder Registry', 'encounters') }}