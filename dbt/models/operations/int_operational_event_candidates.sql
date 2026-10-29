with ranked as (
    select
        event_id, customer_id, event_type, event_timestamp,
        case when length(location_code) <= 30 then location_code else null end as location_code,
        coalesce(length(location_code) > 30, false) as location_code_invalid,
        details, run_id, batch_id, staged_row_id,
        count(*) over (partition by event_id) as source_row_count,
        row_number() over (
            partition by event_id
            order by event_timestamp desc nulls last, staged_row_id desc
        ) as row_number
    from {{ ref('stg_operational_events') }}
    where event_id is not null
)
select
    ranked.event_id,
    ranked.customer_id,
    ranked.event_type,
    ranked.event_timestamp,
    ranked.location_code,
    ranked.location_code_invalid,
    ranked.details,
    ranked.run_id,
    ranked.batch_id,
    ranked.source_row_count,
    customer.customer_id is not null as customer_resolved
from ranked
left join {{ ref('dim_customer') }} customer
  on customer.customer_id = ranked.customer_id
where ranked.row_number = 1
