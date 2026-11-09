{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='fail',
    post_hook="create unique index if not exists uq_fact_transaction_transaction_id on {{ this }} (transaction_id)"
) }}

with approved_batch as (
    select * from {{ source('warehouse_control', 'batch_run') }}
),
transaction_candidates as (
    select * from {{ ref('int_transaction_candidates') }}
)
select
    transaction_id,
    customer_id,
    transaction_timestamp,
    amount::numeric(18, 2) as amount,
    currency_code,
    transaction_status,
    payment_method,
    source_updated_at,
    run_id,
    batch_id,
    current_timestamp as loaded_at
from transaction_candidates
inner join approved_batch
  using (run_id, batch_id)
where transaction_id is not null
  and approved_batch.status = 'SUCCEEDED'
  and customer_id is not null
  and customer_resolved
  and transaction_timestamp is not null
  and amount is not null and amount >= 0
  and currency_code ~ '^[A-Z]{3}$'
  and transaction_status in ('pending', 'completed', 'failed', 'refunded')
  and source_updated_at is not null
