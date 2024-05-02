select
    pat_id,

    cast(birth_date as date) as birth_date,

    datediff('year', birth_date, current_date) as age_yrs,

    coalesce(pat_sex, 'unknown') as sex,

    case
        when ethnic_group_nm = 'Hispanic, Latino or Spanish ethnicity'
            then 'hispanic'
        when patient_race like '%black%'
            then 'nh black'
        when
            patient_race like 'asian%'
            or patient_race like '%indian%'
            or patient_race like '%hawaiian%'
            or patient_race like '%other%'
            then 'nh other'
        when patient_race like '%caucasian%'
            then 'nh white'
        else 'unknown'
    end as raceth,

    case
        when language_nm = 'English'
            then 'english'
        when language_nm = 'Spanish'
            then 'spanish'
        when
            language_nm in (
                'Deaf (none ASL)',
                'American Sign Language'
            )
            then 'american sign language'
        when
            language_nm = 'Unknown'
            or language_nm is null
            then 'unknown'
        else 'other'
    end as language_primary,

    case
        when
            marital_status_nm in (
                'Divorced',
                'Legally Separated',
                'Single',
                'Windowed'
            )
            then 'single'
        when
            marital_status_nm in (
                'Common Law',
                'Life Partner',
                'Married',
                'Significant Other'
            )
            then 'in a relationship'
        when marital_status_nm = 'Other'
            then 'other'
        else 'unknown'
    end as relationship_status

from {{ source('Substance Use Disorder Registry', 'patients') }}
