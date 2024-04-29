{{ config(materialized='table') }}

SELECT PAT_ID,
    CAST(BIRTH_DATE as date) BIRTH_DATE,
    DATEDIFF('year', BIRTH_DATE, CURRENT_DATE) AS AGE_YRS,
    COALESCE(PAT_SEX, 'Unknown') AS SEX,
    CASE
        WHEN ETHNIC_GROUP_NM = 'Hispanic, Latino or Spanish ethnicity' THEN 'Hispanic'
        WHEN PATIENT_RACE LIKE '%BLACK%' THEN 'NH Black'
        WHEN (
            PATIENT_RACE LIKE 'ASIAN%'
            OR PATIENT_RACE LIKE '%INDIAN%'
            OR PATIENT_RACE LIKE '%HAWAIIAN%'
            OR PATIENT_RACE LIKE '%OTHER%'
        ) THEN 'NH Other'
        WHEN PATIENT_RACE LIKE '%CAUCASIAN%' THEN 'NH White'
        ELSE 'Unknown'
    END AS RACETH,
    CASE
        WHEN LANGUAGE_NM = 'English' THEN 'English'
        WHEN LANGUAGE_NM = 'Spanish' THEN 'Spanish'
        WHEN LANGUAGE_NM IN (
            'Deaf (none ASL)',
            'American Sign Language'
        ) THEN 'American Sign Language'
        WHEN LANGUAGE_NM = 'Unknown'
        OR LANGUAGE_NM IS NULL THEN 'Unknown'
        ELSE 'Other'
    END AS LANGUAGE_PRIMARY,
    CASE
        WHEN MARITAL_STATUS_NM IN (
            'Divorced',
            'Legally Separated',
            'Single',
            'Windowed'
        ) THEN 'Single'
        WHEN MARITAL_STATUS_NM IN (
            'Common Law',
            'Life Partner',
            'Married',
            'Significant Other'
        ) THEN 'In a relationship'
        WHEN MARITAL_STATUS_NM = 'Other' THEN 'Other'
        ELSE 'Unknown'
    END AS RELATIONSHIP_STATUS
FROM {{ source('Substance Use Disorder Registry', 'patients') }}