SELECT projection_sql
FROM admin.squark_vertica_projections s
-- after updating squark_vertica_projections to be per-table only, can INNER JOIN to filter on only tables that exist
--INNER JOIN all_tables a ON a.schema_name = s.schema_name AND a.table_name = s.table_names
WHERE 1=1
    AND s.is_enabled = 'True'
    AND s.schema_name = :VERTICA_SCHEMA