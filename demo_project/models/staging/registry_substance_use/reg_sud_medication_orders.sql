select
    pat_id,
    pat_enc_csn_id,
    order_inst,

    cast(start_date as date) as start_date,

    cast(end_date as date) as end_date,

    discon_time,
    ordering_mode_nm,
    order_status_nm,
    order_class_nm,
    atc_code,
    atc_title

from {{ source('Substance Use Disorder Registry', 'medications') }}

where order_status_nm != 'Canceled'
