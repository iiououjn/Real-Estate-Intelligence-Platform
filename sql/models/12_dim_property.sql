-- Junk/attribute dimension: one row per distinct combination of descriptive property attributes.
DELETE FROM dim_property;
INSERT INTO dim_property (property_key, property_type, property_sub_type, property_usage, reg_type, bedrooms, rooms_label)
SELECT ROW_NUMBER() OVER (ORDER BY property_type, property_sub_type, property_usage, reg_type, bedrooms, rooms_label),
       property_type, property_sub_type, property_usage, reg_type, bedrooms, rooms_label
FROM (
  SELECT COALESCE(property_type, 'Unknown') AS property_type, COALESCE(property_sub_type, 'Unknown') AS property_sub_type,
         COALESCE(property_usage, 'Unknown') AS property_usage, COALESCE(reg_type, 'Unknown') AS reg_type,
         COALESCE(bedrooms, -1) AS bedrooms, COALESCE(rooms, 'Unknown') AS rooms_label
  FROM stg_transactions
  UNION
  SELECT COALESCE(property_type, 'Unknown'), COALESCE(property_sub_type, 'Unknown'),
         COALESCE(property_usage, 'Unknown'), 'Unknown', COALESCE(bedrooms, -1), COALESCE(rooms, 'Unknown')
  FROM stg_rentals
) d;
