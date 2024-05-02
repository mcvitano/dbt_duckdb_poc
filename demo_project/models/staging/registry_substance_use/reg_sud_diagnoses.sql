with

diagnosis_dates_wide as (

    select
        pat_id,
        pat_enc_csn_id,
        current_icd10_list,

        -- alcohol
        case
            when current_icd10_list like '%F10%'
                then contact_date
        end as alcohol,

        -- opioid
        case
            when
                current_icd10_list like '%F11%'
                or dx_name like '%opiate%'
                or dx_name like '%opioid%'
                or dx_name like '%methadone%'
                or dx_name like '%heroin%'
                or dx_name like '%fentanyl%'
                then contact_date
        end as opioid,

        -- cannabis
        case
            when current_icd10_list like '%F12%'
                then contact_date
        end as cannabis,

        -- sedative
        case
            when current_icd10_list like '%F13%'
                then contact_date
        end as sedative,

        -- cocaine
        case
            when current_icd10_list like '%F14%'
                then contact_date
        end as cocaine,

        -- other stimulant (if not methamphetamine)
        case
            when
                current_icd10_list like '%F15%'
                and dx_name not like '%methamp%'
                then contact_date
        end as other_stimulant,

        -- methamphetamine
        case
            when dx_name like '%methamp%'
                then contact_date
        end as methamphetamine,

        -- hallucinogen
        case
            when current_icd10_list like '%F16%'
                then contact_date
        end as hallucinogen,

        -- nicotine
        case
            when current_icd10_list like '%F17%'
                then contact_date
        end as nicotine,

        -- inhalant
        case
            when current_icd10_list like '%F18%'
                then contact_date
        end as inhalant,

        -- other psychoactive (if not opioid or methamphetamine)
        case
            when
                current_icd10_list like '%F19%'
                and dx_name not like '%opiate%'
                and dx_name not like '%opioid%'
                and dx_name not like '%methadone%'
                and dx_name not like '%heroin%'
                and dx_name not like '%fentanyl%'
                and dx_name not like '%methamp%'
                then contact_date
        end as other_psychoactive

    from {{ source('Substance Use Disorder Registry', 'diagnoses') }}
),

-- un-pivot the diagnosis dates to long-form
unpivoted_dates as (

    unpivot diagnosis_dates_wide

    on
    alcohol,
    opioid,
    cannabis,
    sedative,
    cocaine,
    other_stimulant,
    methamphetamine,
    hallucinogen,
    nicotine,
    inhalant,
    other_psychoactive

    into
    name substance_type
    value dx_date
),

-- format the dx_date field and add an overdose indicator
formatted_long_diagnosis_dates as (

    select
        pat_id,
        pat_enc_csn_id,
        substance_type,
        current_icd10_list as icd10,

        cast(dx_date as date) as dx_date,

        case
            when current_icd10_list like '%T.%'
                then 'y'
            else 'n'
        end as overdose_yn

    from unpivoted_dates
)

select * from formatted_long_diagnosis_dates
