DELETE FROM dim_project;
INSERT INTO dim_project (project_key, project_name, master_project, project_status, percent_completed)
SELECT ROW_NUMBER() OVER (ORDER BY p.project_name, p.master_project),
       p.project_name, p.master_project, sp.project_status, sp.percent_completed
FROM (
  SELECT COALESCE(project_name, 'Unknown') AS project_name, COALESCE(master_project, 'Unknown') AS master_project FROM stg_transactions
  UNION
  SELECT COALESCE(project_name, 'Unknown'), COALESCE(master_project, 'Unknown') FROM stg_rentals
) p
LEFT JOIN (
  SELECT project_name, MAX(project_status) AS project_status, MAX(percent_completed) AS percent_completed
  FROM stg_projects GROUP BY project_name
) sp ON sp.project_name = p.project_name;
