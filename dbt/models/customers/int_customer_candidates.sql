with crm_ranked as (
    select *, row_number() over (
        partition by customer_id
        order by updated_at desc nulls last, staged_row_id desc
    ) as row_number
    from {{ ref('stg_customers') }}
    where customer_id is not null
),
api_ranked as (
    select *, row_number() over (
        partition by customer_id
        order by status_updated_at desc nulls last, staged_row_id desc
    ) as row_number
    from {{ ref('stg_customer_statuses') }}
    where customer_id is not null
),
candidates as (
    select
        crm.customer_id, crm.first_name, crm.last_name, crm.email, crm.phone,
        crm.country_code,
        case when api.status_updated_at >= crm.updated_at then api.status else crm.status end as status,
        case when api.status_updated_at >= crm.updated_at then 'STATUS_API' else 'CRM_PG' end as status_source,
        crm.created_at as source_created_at,
        case when api.status_updated_at >= crm.updated_at then api.status_updated_at else crm.updated_at end as source_updated_at,
        case when api.status_updated_at >= crm.updated_at then api.run_id else crm.run_id end as run_id,
        case when api.status_updated_at >= crm.updated_at then api.batch_id else crm.batch_id end as batch_id
    from crm_ranked crm
    left join api_ranked api on api.customer_id = crm.customer_id and api.row_number = 1
    where crm.row_number = 1
)
select * from candidates
