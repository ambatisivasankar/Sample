SELECT s.projection_sql
FROM admin.squark_vertica_projections s
-- only add projections if related table exists in this schema
INNER JOIN all_tables a ON a.schema_name = s.schema_name AND a.table_name = s.table_name
WHERE 1=1
    AND s.is_enabled = 'True'
    AND s.schema_name = :VERTICA_SCHEMA
ORDER BY s.id