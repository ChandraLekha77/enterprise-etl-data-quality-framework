{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    on_schema_change='fail',
    post_hook="create unique index if not exists uq_dim_customer_customer_id on {{ this }} (customer_id)"
) }}

with approved_batch as (
    select * from {{ source('warehouse_control', 'batch_run') }}
),
customer_candidates as (
    select * from {{ ref('int_customer_candidates') }}
)
select
    candidate.customer_id, candidate.first_name, candidate.last_name,
    candidate.email, candidate.phone, candidate.country_code, candidate.status,
    candidate.status_source, candidate.source_created_at,
    candidate.source_updated_at, candidate.run_id, candidate.batch_id,
    current_timestamp as loaded_at
from customer_candidates candidate
inner join approved_batch
  using (run_id, batch_id)
where candidate.customer_id is not null
  and approved_batch.status = 'SUCCEEDED'
  and candidate.first_name is not null
  and candidate.last_name is not null
  and candidate.email is not null
  and candidate.email ~ '^[^@[:space:]]+@[^@[:space:]]+$'
  and candidate.country_code ~ '^[A-Z]{2}$'
  and candidate.status in ('active', 'inactive', 'suspended')
  and candidate.source_created_at is not null
  and candidate.source_updated_at is not null
  and candidate.source_created_at <= candidate.source_updated_at
