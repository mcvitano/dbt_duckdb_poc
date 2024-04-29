{{ config(materialized='table') }}


with jps_start_encs as (
    SELECT PAT_ID, PAT_ENC_CSN_ID, DAY_OF_WEEK, YEAR,
        APPT_TIME, APPT_STATUS_NM, RSN_FOR_VISIT_LIST, OTHER_RSN_LIST,
        INSURANCE_CLASS
    FROM {{ ref('reg_sud_encounters') }}
    WHERE DEPARTMENT_NAME == 'JPS START'
),
ed_encs as (
    SELECT PAT_ID, PAT_ENC_CSN_ID,
        EMER_ADM_DATE,
        CASE
            WHEN INP_ADM_DATE IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS EMER_HOSPITALIZED_YN,
        HOSP_DISCH_TIME AS EMER_DISCH_TIME,
        RSN_FOR_VISIT_LIST AS EMER_RSN_VISIT, 
        OTHER_RSN_LIST AS EMER_RSN_OTHER
    FROM {{ ref('reg_sud_encounters') }}
    WHERE EMER_ADM_DATE IS NOT NULL or
        DEPARTMENT_NAME = 'JPS EMERGENCY' or
        DEPARTMENT_NAME = 'JPS URGENT CARE CENTER MAIN'
),
bridge_enrollment as (
    SELECT PAT_ID,
        ENROLL_STATUS_NAME AS BRIDGE_STATUS,
        ENROLL_START_DT AS BRIDGE_START_DATE,
        ENROLL_END_DT AS BRIDGE_END_DATE
    FROM {{ source('Substance Use Disorder Registry', 'bridge_enrollment') }}
),
bup_orders as (
    SELECT PAT_ID, 
        ORDER_DATE AS BUP_PREVIOUS_DATE
    FROM source('Substance Use Disorder Registry', 'medication_orders')
    WHERE ATC_CODE IN (N07BC51, N07BC01)
),
merged_one_to_many as (
    SELECT obot.PAT_ID, obot.PAT_ENC_CSN_ID, DAY_OF_WEEK, YEAR,
        APPT_TIME, APPT_STATUS_NM, RSN_FOR_VISIT_LIST, OTHER_RSN_LIST,
        INSURANCE_CLASS,
        EMER_ADM_DATE, EMER_HOSPITALIZED_YN, EMER_DISCH_TIME,
        EMER_RSN_VISIT, EMER_RSN_OTHER,
        BRIDGE_STATUS, BRIDGE_START_DATE, BRIDGE_END_DATE,
        row_number() over(partition by obot.PAT_ENC_CSN_ID order by ed.EMER_ADM_DATE desc) as obot_ed_seq
    FROM jps_start_encs as obot
    LEFT JOIN ed_encs as ed
        ON obot.PAT_ID = ed.PAT_ID
    LEFT JOIN bridge_enrollment as bridge
        ON obot.PAT_ID = bridge.PAT_ID
    LEFT JOIN bup_orders as bup
        ON obot.PAT_ID = bup.PAT_ID
    WHERE 1=1
        AND ed.EMER_ADM_DATE < obot.APPT_TIME
        AND bup.BUP_PREVIOUS_DATE < obot.APPT_TIME
)
-- keep single (previous) ED visit per-OBOT encounter (in case multiple)
SELECT PAT_ID, PAT_ENC_CSN_ID, DAY_OF_WEEK, YEAR,
        APPT_TIME, APPT_STATUS_NM, RSN_FOR_VISIT_LIST, OTHER_RSN_LIST,
        INSURANCE_CLASS,
        BUP_PREVIOUS_DATE,
        EMER_ADM_DATE, EMER_HOSPITALIZED_YN, EMER_DISCH_TIME,
        EMER_RSN_VISIT, EMER_RSN_OTHER,
        BRIDGE_STATUS, BRIDGE_START_DATE, BRIDGE_END_DATE
FROM merged_one_to_many
WHERE obot_ed_seq = 1
