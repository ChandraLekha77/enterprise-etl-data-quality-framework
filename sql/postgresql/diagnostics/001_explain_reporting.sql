-- Run manually against a populated non-production warehouse.
-- These predicates mirror the dashboard's frequent reporting calls.

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT * FROM control.batch_run
WHERE run_id = '00000000-0000-0000-0000-000000000000'::uuid
ORDER BY created_at DESC LIMIT 100;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT source_system, rule_id, severity, COUNT(*)
FROM quality.validation_result
WHERE run_id = '00000000-0000-0000-0000-000000000000'::uuid
GROUP BY source_system, rule_id, severity;

EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT rule_id, status, group_key, source_value, target_value
FROM quality.reconciliation_result
WHERE run_id = '00000000-0000-0000-0000-000000000000'::uuid AND status = 'FAIL'
ORDER BY created_at DESC LIMIT 100;
