{{ config(materialized = 'table') }}

WITH dx_dates_wide AS (
    SELECT PAT_ID,
        PAT_ENC_CSN_ID,
        CURRENT_ICD10_LIST,
        -- Alcohol
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F10%' THEN CONTACT_DATE
            ELSE NULL
        END AS Alcohol,
        -- Opioid
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F11%'
            OR DX_NAME LIKE '%Opiate%'
            OR DX_NAME LIKE '%Opioid%'
            OR DX_NAME LIKE '%Methadone%'
            OR DX_NAME LIKE '%Heroin%'
            OR DX_NAME LIKE '%Fentanyl%' THEN CONTACT_DATE
            ELSE NULL
        END AS Opioid,
        -- Cannabis
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F12%' THEN CONTACT_DATE
            ELSE NULL
        END AS Cannabis,
        -- Sedative
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F13%' THEN CONTACT_DATE
            ELSE NULL
        END AS Sedative,
        -- Cocaine
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F14%' THEN CONTACT_DATE
            ELSE NULL
        END AS Cocaine,
        -- Other Stimulant (if not Methamphetamine)
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F15%'
            AND DX_NAME NOT LIKE '%Methamp%' THEN CONTACT_DATE
            ELSE NULL
        END AS Other_Stimulant,
        -- Methamphetamine
        CASE
            WHEN DX_NAME LIKE '%Methamp%' THEN CONTACT_DATE
            ELSE NULL
        END AS Methamphetamine,
        -- Hallucinogen
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F16%' THEN CONTACT_DATE
            ELSE NULL
        END AS Hallucinogen,
        -- Nicotine
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F17%' THEN CONTACT_DATE
            ELSE NULL
        END AS Nicotine,
        -- Inhalant
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F18%' THEN CONTACT_DATE
            ELSE NULL
        END AS Inhalant,
        -- Other Psychoactive (if not opioid or methamphetamine)
        CASE
            WHEN CURRENT_ICD10_LIST LIKE '%F19%'
            AND DX_NAME NOT LIKE '%Opiate%'
            AND DX_NAME NOT LIKE '%Opioid%'
            AND DX_NAME NOT LIKE '%Methadone%'
            AND DX_NAME NOT LIKE '%Heroin%'
            AND DX_NAME NOT LIKE '%Fentanyl%'
            AND DX_NAME NOT LIKE '%Methamp%' THEN CONTACT_DATE
            ELSE NULL
        END AS Other_Psychoactive
    FROM {{ source('Substance Use Disorder Registry', 'diagnoses') }}
),
-- Un-pivot the data
unpivoted_dates AS (
    UNPIVOT dx_dates_wide 
    ON Alcohol,
        Opioid,
        Cannabis,
        Sedative,
        Cocaine,
        Other_Stimulant,
        Methamphetamine,
        Hallucinogen,
        Nicotine,
        Inhalant,
        Other_Psychoactive
    INTO
        NAME SUBSTANCE_TYPE
        VALUE DX_DATE
)
SELECT PAT_ID,
    PAT_ENC_CSN_ID,
    CURRENT_ICD10_LIST AS ICD10,
    CASE
        WHEN CURRENT_ICD10_LIST LIKE '%T.%' THEN 'Y'
        ELSE 'N'
    END AS OVERDOSE_YN,
    SUBSTANCE_TYPE,
    CAST(DX_DATE as date) DX_DATE
FROM unpivoted_dates