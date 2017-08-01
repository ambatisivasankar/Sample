WITH cteAllTables AS
(
	SELECT s.schema_name, table_name, table_type, s.create_time AS schema_create_time
	FROM all_tables t
	INNER JOIN schemata s ON s.schema_name = t.schema_name
	WHERE 1=1
		AND table_type = 'TABLE'
		AND s.schema_name = :VERTICA_SCHEMA
	ORDER BY schema_name, table_name
)
,cteDeleted AS
(
	-- returns all projection_names, e.g. yadda_b0 and yadda_b1, later join will discard the dupes
	SELECT A.schema_name, A.projection_name, A.deleted_row_count
	FROM (
		SELECT schema_name, projection_name, SUM(deleted_row_count) as deleted_row_count
		FROM delete_vectors
		GROUP BY schema_name, projection_name
	) A
)
,cteUnadjustedRowCounts AS
(
	SELECT A.schema_name, A.anchor_table_name, A.projection_name, SUM(A.row_count) as total_row_count
	FROM (
	SELECT node_name,
		projection_schema AS schema_name,
		anchor_table_name,
		projection_name,
		row_count,
	ROW_NUMBER() OVER (PARTITION BY node_name, projection_schema, anchor_table_name ORDER BY projection_name) AS row_num
	FROM projection_storage ps
	) A
	WHERE A.row_num = 1
	GROUP BY A.schema_name, A.anchor_table_name, A.projection_name
)
,cteFinalRowCounts AS
(
	SELECT u.schema_name, u.projection_name, u.anchor_table_name,
		u.total_row_count - COALESCE(d.deleted_row_count, 0) AS final_row_count,
		CASE WHEN u.total_row_count - COALESCE(d.deleted_row_count, 0) = 0 THEN 0 ELSE 1 END as has_data,
		d.deleted_row_count, u.total_row_count
	FROM cteUnadjustedRowCounts u
	LEFT JOIN cteDeleted d ON d.schema_name = u.schema_name AND d.projection_name = u.projection_name
)
,cteCombined AS
(
	SELECT a.schema_name, a.table_name, f.final_row_count as row_count, a.schema_create_time, COUNT(*) OVER (PARTITION BY a.schema_name) AS TableCnt,
		SUM(f.has_data) OVER (PARTITION BY a.schema_name) as PopdTblsCount
	FROM cteAllTables a
	LEFT JOIN cteFinalRowCounts f ON f.schema_name = a.schema_name AND f.anchor_table_name = a.table_name
)
,cteSourceCounts AS
(
	SELECT A.project_id, A.source_schema, A.table_name, A.row_count, A.query_date, A.build_number, A.job_name, A.seconds_query_duration
	FROM (
		-- below is more flexible vs. passing in build number as a variable..
		SELECT src.*, MAX(build_number) OVER (PARTITION BY src.project_id) as lastProjectBuild
		FROM admin.squark_source_row_counts src
		WHERE 1=1
			AND src.project_id IN (SELECT DISTINCT a.schema_name FROM cteAllTables a)
			AND src.query_date > SYSDATE() - 10
	) A
	WHERE A.build_number = lastProjectBuild
)
SELECT c.schema_name, c.table_name, c.row_count as VERT_row_count, src.row_count as SourceRowCount,
	COALESCE(src.row_count, 0) - COALESCE(c.row_count, 0) as DIFF, CAST(CAST(src.seconds_query_duration as INT) as VARCHAR(10)) || ' secs' as querySecs,
	c.schema_create_time, c.TableCnt,
	CAST(c.PopdTblsCount AS VARCHAR(10))  || ' of ' || CAST(TableCnt AS VARCHAR(10)) as PopdTbls,	src.build_number, src.query_date as SourceQueryTime,
	CASE WHEN src.row_count IS NULL THEN 'n.a.'
		  WHEN COALESCE(src.row_count, 0) - COALESCE(c.row_count, 0) <> 0 THEN '>>>> WHOOPS row count <<<<'
		  ELSE '.'
	END as RowCountCheck
FROM cteCombined c
LEFT JOIN cteSourceCounts src ON src.project_id = c.schema_name AND src.table_name = c.table_name
WHERE 1 = 1
ORDER BY c.schema_name, c.table_name

