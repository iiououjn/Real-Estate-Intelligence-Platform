-- One row per pipeline run and dataset: volumes, reject rate and how many quality checks failed / warned.
DELETE FROM mart_pipeline_health;
INSERT INTO mart_pipeline_health (run_id, dataset, rows_in, rows_out, rows_rejected, reject_pct, checks_failed, checks_warned, finished_at)
SELECT p.run_id, p.dataset, p.rows_in, p.rows_out, p.rows_rejected,
       CASE WHEN p.rows_in > 0 THEN 100.0 * p.rows_rejected / p.rows_in END,
       COALESCE(c.failed, 0), COALESCE(c.warned, 0), p.finished_at
FROM pipeline_run_log p
LEFT JOIN (
  SELECT run_id, dataset,
         SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
         SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warned
  FROM dq_run_log GROUP BY run_id, dataset
) c ON c.run_id = p.run_id AND c.dataset = p.dataset
WHERE p.stage IN ('transform', 'warehouse_checks');
