{{ config(materialized='view') }}

select

    department_name,
    raceth,
    year(cast(contact_date as date)) as contact_year,
    count(*) as unique_patient_count

from {{ source("Global Registry", 'pcmh_visits_denominators_2018_2023') }}

-- Cube() function yields [ambiguous.column_references] from sqlfluff
group by cube(1, 2, 3)

order by 1, 2, 3
