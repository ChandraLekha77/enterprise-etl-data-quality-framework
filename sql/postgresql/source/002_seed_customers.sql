BEGIN;

INSERT INTO crm.customers (
    source_row_id,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    country_code,
    status,
    created_at,
    updated_at
)
VALUES
    (1, 'C000001', 'Ava', 'Patel', 'ava.patel@example.com', '+1-202-555-0101', 'US', 'active', '2026-01-10T14:00:00Z', '2026-10-18T09:00:00Z'),
    (2, ' C000002 ', ' Noah ', 'Kim', 'NOAH.KIM@EXAMPLE.COM ', NULL, ' ca ', 'ACTIVE', '2026-02-11T15:30:00Z', '2026-10-17T10:00:00Z'),
    (3, 'C000003', 'Mia', 'Garcia', 'mia.garcia@example.com', '+1-202-555-0103', 'GB', 'suspended', '2026-03-12T08:15:00Z', '2026-10-20T12:00:00Z'),
    (4, 'C000004', 'Liam', 'Brown', 'liam.brown.example.com', '+1-202-555-0104', 'US', 'active', '2026-04-13T09:45:00Z', '2026-10-19T11:00:00Z'),
    (5, 'C000005', 'Emma', 'Wilson', 'emma.wilson@example.com', '+1-202-555-0105', 'USA', 'active', '2026-05-14T10:20:00Z', '2026-10-19T13:00:00Z'),
    (6, 'C000006', 'Oliver', 'Davis', 'oliver.davis@example.com', '+1-202-555-0106', 'US', 'inactive', '2026-10-21T10:00:00Z', '2026-10-20T10:00:00Z'),
    (7, 'C000002', 'Noah', 'Kim', 'noah.kim@example.com', '+1-202-555-0102', 'CA', 'active', '2026-02-11T15:30:00Z', '2026-10-20T14:00:00Z')
ON CONFLICT (source_row_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    country_code = EXCLUDED.country_code,
    status = EXCLUDED.status,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;

SELECT setval(
    pg_get_serial_sequence('crm.customers', 'source_row_id'),
    COALESCE((SELECT MAX(source_row_id) FROM crm.customers), 1),
    TRUE
);

COMMIT;
