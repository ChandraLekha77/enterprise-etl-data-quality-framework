select
    staged_row_id,
    {{ safe_text('event_id') }} as event_id, event_id as event_id_original,
    {{ safe_text('customer_id') }} as customer_id, customer_id as customer_id_original,
    lower({{ safe_text('event_type') }}) as event_type, event_type as event_type_original,
    {{ safe_timestamptz('event_timestamp_raw') }} as event_timestamp,
    event_timestamp_raw as event_timestamp_original,
    {{ safe_text('location_code') }} as location_code, location_code as location_code_original,
    {{ safe_text('details') }} as details, details as details_original,
    source_file_name, source_file_checksum, run_id, batch_id, source_system,
    source_record_id, source_updated_at, record_checksum, ingested_at
from {{ source('warehouse_staging', 'operational_event_raw') }}
