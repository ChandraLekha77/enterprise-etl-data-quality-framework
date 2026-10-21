USE billing_source;
GO

SET IDENTITY_INSERT dbo.transactions ON;
GO

MERGE dbo.transactions AS target
USING (
    VALUES
        (CAST(1 AS BIGINT), N'T000001', N'C000001', CAST('2026-10-20T10:00:00+00:00' AS DATETIMEOFFSET), CAST(100.00 AS DECIMAL(18, 2)), N'USD', N'completed', N'card', CAST('2026-10-20T10:01:00+00:00' AS DATETIMEOFFSET)),
        (2, N'T000002', N'C000002', CAST('2026-10-20T11:00:00+00:00' AS DATETIMEOFFSET), CAST(250.50 AS DECIMAL(18, 2)), N'usd', N'COMPLETED', N'bank_transfer', CAST('2026-10-20T11:01:00+00:00' AS DATETIMEOFFSET)),
        (3, N'T000003', N'C000003', CAST('2026-10-20T12:00:00+00:00' AS DATETIMEOFFSET), CAST(40.00 AS DECIMAL(18, 2)), N'USD', N'refunded', N'card', CAST('2026-10-20T12:01:00+00:00' AS DATETIMEOFFSET)),
        (4, N'T000004', N'C000001', CAST('2026-10-20T13:00:00+00:00' AS DATETIMEOFFSET), CAST(75.00 AS DECIMAL(18, 2)), N'EUR', N'pending', NULL, CAST('2026-10-20T13:01:00+00:00' AS DATETIMEOFFSET)),
        (5, N'T000005', N'C000002', CAST('2026-10-20T14:00:00+00:00' AS DATETIMEOFFSET), CAST(-10.00 AS DECIMAL(18, 2)), N'USD', N'completed', N'card', CAST('2026-10-20T14:01:00+00:00' AS DATETIMEOFFSET)),
        (6, N'T000006', N'C999999', CAST('2026-10-20T15:00:00+00:00' AS DATETIMEOFFSET), CAST(25.00 AS DECIMAL(18, 2)), N'USD', N'completed', N'card', CAST('2026-10-20T15:01:00+00:00' AS DATETIMEOFFSET)),
        (7, N'T000007', N'C000003', CAST('2026-10-20T16:00:00+00:00' AS DATETIMEOFFSET), CAST(15.00 AS DECIMAL(18, 2)), N'US', N'completed', N'cash', CAST('2026-10-20T16:01:00+00:00' AS DATETIMEOFFSET)),
        (8, N'T000008', N'C000001', CAST('2026-10-20T17:00:00+00:00' AS DATETIMEOFFSET), CAST(120.00 AS DECIMAL(18, 2)), N'USD', N'pending', N'card', CAST('2026-10-20T17:01:00+00:00' AS DATETIMEOFFSET)),
        (9, N'T000008', N'C000001', CAST('2026-10-20T17:00:00+00:00' AS DATETIMEOFFSET), CAST(125.00 AS DECIMAL(18, 2)), N'USD', N'completed', N'card', CAST('2026-10-20T18:01:00+00:00' AS DATETIMEOFFSET))
) AS source (
    source_row_id,
    transaction_id,
    customer_id,
    transaction_timestamp,
    amount,
    currency_code,
    transaction_status,
    payment_method,
    updated_at
)
ON target.source_row_id = source.source_row_id
WHEN MATCHED THEN UPDATE SET
    transaction_id = source.transaction_id,
    customer_id = source.customer_id,
    transaction_timestamp = source.transaction_timestamp,
    amount = source.amount,
    currency_code = source.currency_code,
    transaction_status = source.transaction_status,
    payment_method = source.payment_method,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (
    source_row_id,
    transaction_id,
    customer_id,
    transaction_timestamp,
    amount,
    currency_code,
    transaction_status,
    payment_method,
    updated_at
)
VALUES (
    source.source_row_id,
    source.transaction_id,
    source.customer_id,
    source.transaction_timestamp,
    source.amount,
    source.currency_code,
    source.transaction_status,
    source.payment_method,
    source.updated_at
);
GO

SET IDENTITY_INSERT dbo.transactions OFF;
GO
