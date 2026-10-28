select
    staged_row_id,
    {{ safe_text('customer_id') }} as customer_id, customer_id as customer_id_original,
    {{ safe_text('first_name') }} as first_name, first_name as first_name_original,
    {{ safe_text('last_name') }} as last_name, last_name as last_name_original,
    lower({{ safe_text('email') }}) as email, email as email_original,
    {{ safe_text('phone') }} as phone, phone as phone_original,
    upper({{ safe_text('country_code') }}) as country_code, country_code as country_code_original,
    lower({{ safe_text('status') }}) as status, status as status_original,
    {{ safe_timestamptz('created_at_raw') }} as created_at, created_at_raw as created_at_original,
    {{ safe_timestamptz('updated_at_raw') }} as updated_at, updated_at_raw as updated_at_original,
    run_id, batch_id, source_system, source_record_id, source_updated_at,
    record_checksum, ingested_at
from {{ source('warehouse_staging', 'customer_raw') }}
