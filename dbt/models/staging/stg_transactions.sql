select
    staged_row_id,
    {{ safe_text('transaction_id') }} as transaction_id, transaction_id as transaction_id_original,
    {{ safe_text('customer_id') }} as customer_id, customer_id as customer_id_original,
    {{ safe_timestamptz('transaction_timestamp_raw') }} as transaction_timestamp,
    transaction_timestamp_raw as transaction_timestamp_original,
    {{ safe_numeric('amount_raw', 18, 2) }} as amount, amount_raw as amount_original,
    upper({{ safe_text('currency_code') }}) as currency_code, currency_code as currency_code_original,
    lower({{ safe_text('transaction_status') }}) as transaction_status,
    transaction_status as transaction_status_original,
    {{ safe_text('payment_method') }} as payment_method, payment_method as payment_method_original,
    {{ safe_timestamptz('updated_at_raw') }} as updated_at, updated_at_raw as updated_at_original,
    run_id, batch_id, source_system, source_record_id, source_updated_at,
    record_checksum, ingested_at
from {{ source('warehouse_staging', 'transaction_raw') }}
