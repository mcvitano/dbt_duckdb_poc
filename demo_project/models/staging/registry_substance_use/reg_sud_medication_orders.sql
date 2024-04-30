{{ config(materialized='table') }}

SELECT PAT_ID, PAT_ENC_CSN_ID, ORDER_INST, 
        cast(START_DATE as date) as START_DATE, 
        cast(END_DATE as date) as END_DATE,
        DISCON_TIME,
        ORDERING_MODE_NM, ORDER_STATUS_NM, ORDER_CLASS_NM,
        ATC_CODE, ATC_TITLE
FROM {{ source('Substance Use Disorder Registry', 'medications') }}
WHERE 1=1
    AND ORDER_STATUS_NM != 'Canceled'