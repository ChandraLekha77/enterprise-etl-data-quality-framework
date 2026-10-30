{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

select
    event_id,
    customer_id,
    event_type,
    event_timestamp,
    location_code,
    details,
    run_id,
    batch_id,
    current_timestamp as loaded_at
from {{ ref('int_operational_event_candidates') }}
inner join {{ source('warehouse_control', 'batch_run') }} approved_batch
  using (run_id, batch_id)
where event_id is not null
  and approved_batch.status = 'SUCCEEDED'
  and customer_id is not null
  and customer_resolved
  and event_timestamp is not null
  and event_type in ('service', 'shipment', 'support', 'maintenance')
