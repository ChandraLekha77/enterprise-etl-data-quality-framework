DELETE FROM control.source_watermark
WHERE source_system IN ('CRM_PG', 'STATUS_API', 'BILLING_MSSQL', 'OPS_CSV');
