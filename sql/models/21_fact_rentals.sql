DELETE FROM fact_rentals;
INSERT INTO fact_rentals (contract_id, start_date_key, end_date_key, area_key, project_key, property_key, contract_reg_type,
                          tenant_type, property_id, area_sqm, area_sqft, annual_rent_aed, annual_rent_per_sqft, duration_days)
SELECT r.contract_id, ds.date_key, de.date_key, a.area_key, p.project_key, pr.property_key, r.contract_reg_type,
       r.tenant_type, r.property_id, r.area_sqm, r.area_sqm * 10.7639, r.annual_amount,
       CASE WHEN r.area_sqm > 0 THEN r.annual_amount / (r.area_sqm * 10.7639) END,
       de.day_num - ds.day_num
FROM stg_rentals r
JOIN dim_date ds ON ds.full_date = r.contract_start_date
LEFT JOIN dim_date de ON de.full_date = r.contract_end_date
JOIN dim_area a ON a.area_name = COALESCE(r.area_name, 'Unknown')
JOIN dim_project p ON p.project_name = COALESCE(r.project_name, 'Unknown') AND p.master_project = COALESCE(r.master_project, 'Unknown')
JOIN dim_property pr ON pr.property_type = COALESCE(r.property_type, 'Unknown')
                    AND pr.property_sub_type = COALESCE(r.property_sub_type, 'Unknown')
                    AND pr.property_usage = COALESCE(r.property_usage, 'Unknown')
                    AND pr.reg_type = 'Unknown'
                    AND pr.bedrooms = COALESCE(r.bedrooms, -1)
                    AND pr.rooms_label = COALESCE(r.rooms, 'Unknown');
