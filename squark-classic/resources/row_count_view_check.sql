
-- output a series of COUNT(*) for all views in a schema, primarily with an eye on the hybrid views being developed for haven
--  if there is a problem with the view, e.g. it is doing a UNION ALL query between two source tables with differing DDL,
--  executing the below query should produce an error
WITH cte AS (
	SELECT table_name, ROW_NUMBER() OVER (ORDER BY UPPER(table_name) DESC) as rowNum
	FROM all_tables
	WHERE schema_name = :VERTICA_SCHEMA AND table_type = 'VIEW'
)
SELECT 'SELECT ''' || table_name || ''' AS table_name, COUNT(*) FROM ' || :VERTICA_SCHEMA || '.' || table_name || CASE WHEN rowNum = 1 THEN '' ELSE ' UNION ALL' END
FROM cte
ORDER BY UPPER(table_name)

