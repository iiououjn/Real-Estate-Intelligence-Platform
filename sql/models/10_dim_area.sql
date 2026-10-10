DELETE FROM dim_area;
INSERT INTO dim_area (area_key, area_name)
SELECT ROW_NUMBER() OVER (ORDER BY area_name), area_name
FROM (
  SELECT COALESCE(area_name, 'Unknown') AS area_name FROM stg_transactions
  UNION
  SELECT COALESCE(area_name, 'Unknown') FROM stg_rentals
  UNION
  SELECT COALESCE(area_name, 'Unknown') FROM stg_projects
) a;
