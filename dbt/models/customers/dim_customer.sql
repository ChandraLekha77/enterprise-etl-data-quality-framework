{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

select
    customer_id, first_name, last_name, email, phone, country_code, status,
    status_source, source_created_at, source_updated_at, run_id, batch_id,
    current_timestamp as loaded_at
from {{ ref('int_customer_candidates') }}
where customer_id is not null
  and first_name is not null
  and last_name is not null
  and email is not null
  and email ~ '^[^@[:space:]]+@[^@[:space:]]+$'
  and country_code ~ '^[A-Z]{2}$'
  and status in ('active', 'inactive', 'suspended')
  and source_created_at is not null
  and source_updated_at is not null
  and source_created_at <= source_updated_at
