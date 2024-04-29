{{ config(materialized='view') }}

SELECT 
    year(cast(CONTACT_DATE as date)) as YEAR,
    DEPARTMENT_NAME,
    RACETH,
    COUNT(*) as UNIQUE_PATIENT_COUNT
FROM {{ source("Global Registry", 'pcmh_visits_denominators_2018_2023') }}
GROUP BY CUBE(1, 2, 3)
ORDER BY (1, 2, 3)