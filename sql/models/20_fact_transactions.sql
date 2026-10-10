-- 1 sq.m = 10.7639 sq.ft. Non-positive areas never reach here (rejected upstream); CASE keeps the SQL safe anyway.
DELETE FROM fact_transactions;
INSERT INTO fact_transactions (transaction_id, date_key, area_key, project_key, property_key, trans_group, procedure_name,
                               property_id, has_parking, area_sqm, area_sqft, amount_aed, price_per_sqft)
SELECT t.transaction_id, d.date_key, a.area_key, p.project_key, pr.property_key, t.trans_group, t.procedure_name,
       t.property_id, t.has_parking, t.area_sqm, t.area_sqm * 10.7639, t.amount_aed,
       CASE WHEN t.area_sqm > 0 THEN t.amount_aed / (t.area_sqm * 10.7639) END
FROM stg_transactions t
JOIN dim_date d ON d.full_date = t.transaction_date
JOIN dim_area a ON a.area_name = COALESCE(t.area_name, 'Unknown')
JOIN dim_project p ON p.project_name = COALESCE(t.project_name, 'Unknown') AND p.master_project = COALESCE(t.master_project, 'Unknown')
JOIN dim_property pr ON pr.property_type = COALESCE(t.property_type, 'Unknown')
                    AND pr.property_sub_type = COALESCE(t.property_sub_type, 'Unknown')
                    AND pr.property_usage = COALESCE(t.property_usage, 'Unknown')
                    AND pr.reg_type = COALESCE(t.reg_type, 'Unknown')
                    AND pr.bedrooms = COALESCE(t.bedrooms, -1)
                    AND pr.rooms_label = COALESCE(t.rooms, 'Unknown');
