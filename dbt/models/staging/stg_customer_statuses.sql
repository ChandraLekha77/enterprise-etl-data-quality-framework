select
    staged_row_id,
    {{ safe_text('customer_id') }} as customer_id, customer_id as customer_id_original,
    lower({{ safe_text('status') }}) as status, status as status_original,
    {{ safe_timestamptz('status_updated_at_raw') }} as status_updated_at,
    status_updated_at_raw as status_updated_at_original,
    page_token, raw_payload, run_id, batch_id, source_system, source_record_id,
    source_updated_at, record_checksum, ingested_at
from {{ source('warehouse_staging', 'customer_status_raw') }}
