with ranked as (
    select
        transaction_id, customer_id, transaction_timestamp, amount,
        currency_code, transaction_status, payment_method, updated_at,
        run_id, batch_id, staged_row_id,
        count(*) over (partition by transaction_id) as source_row_count,
        row_number() over (
            partition by transaction_id
            order by updated_at desc nulls last, staged_row_id desc
        ) as row_number
    from {{ ref('stg_transactions') }}
    where transaction_id is not null
)
select
    ranked.transaction_id,
    ranked.customer_id,
    ranked.transaction_timestamp,
    ranked.amount,
    ranked.currency_code,
    ranked.transaction_status,
    ranked.payment_method,
    ranked.updated_at as source_updated_at,
    ranked.run_id,
    ranked.batch_id,
    ranked.source_row_count,
    customer.customer_id is not null as customer_resolved
from ranked
left join {{ ref('dim_customer') }} customer
  on customer.customer_id = ranked.customer_id
where ranked.row_number = 1
