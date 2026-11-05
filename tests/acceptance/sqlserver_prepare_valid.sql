USE billing_source;
DELETE FROM dbo.transactions WHERE source_row_id IN (5, 6, 7);
