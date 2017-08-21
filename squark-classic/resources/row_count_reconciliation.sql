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
-- select * from cteDeleted ORDER BY schema_name, projection_name
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
--SELECT * FROM cteUnadjustedRowCounts
--WHERE schema_name = 'esp' ORDER BY schema_name, anchor_table_name
,cteFinalRowCounts AS
(
	SELECT u.schema_name, u.projection_name, u.anchor_table_name,
		u.total_row_count - COALESCE(d.deleted_row_count, 0) AS final_row_count,
		CASE WHEN u.total_row_count - COALESCE(d.deleted_row_count, 0) = 0 THEN 0 ELSE 1 END as has_data,
		d.deleted_row_count, u.total_row_count
	FROM cteUnadjustedRowCounts u
	LEFT JOIN cteDeleted d ON d.schema_name = u.schema_name AND d.projection_name = u.projection_name
)
--SELECT * FROM cteFinalRowCounts ORDER BY schema_name, anchor_table_name
,cteCombined AS 
(
	SELECT a.schema_name, a.table_name, f.final_row_count as row_count, a.schema_create_time, COUNT(*) OVER (PARTITION BY a.schema_name) AS TableCnt,
		SUM(f.has_data) OVER (PARTITION BY a.schema_name) as PopdTblsCount
	FROM cteAllTables a
	LEFT JOIN cteFinalRowCounts f ON f.schema_name = a.schema_name AND f.anchor_table_name = a.table_name
)
--SELECT * FROM cteCombined ORDER BY schema_name, table_name
,cteSourceCounts AS
(
	SELECT A.project_id, A.source_schema, A.table_name, A.row_count, A.query_date, A.build_number, A.job_name, A.seconds_query_duration, A.is_after, A.rowCount
	FROM (
		SELECT src.*, MAX(build_number) OVER (PARTITION BY src.project_id, src.job_name) as lastProjectBuild --fix2
			-- on the off chance there are two diff jobs writing to same schema, e.g. a multi-job and a normal job, need something like below to pull out the matching rows
			-- what will be reported upon will be latest data loaded in, irrespective of per-job build numbering
			,ROW_NUMBER() OVER (PARTITION BY src.project_id, src.table_name ORDER BY src.query_date DESC) as rowCount
		FROM admin.squark_source_row_counts src
		WHERE 1=1
			AND src.project_id IN (SELECT DISTINCT a.schema_name FROM cteAllTables a)
			AND src.query_date > SYSDATE() - 10
--			AND src.job_name = :JOB_NAME  -- this approach fails w/multi-jobs, at least w/o a bunch more work
	) A
	WHERE A.build_number = lastProjectBuild
		AND A.rowCount <= 2 -- rowCount=1 will be for is_after=true, rowCount=2 will be for is_after=false
)
--SELECT * FROM cteSourceCounts ORDER BY project_id, table_name   
,cteAmalgSourceCounts AS
(

	SELECT scb.project_id, scb.source_schema, scb.table_name, scb.query_date as beforeQueryTime, scb.build_number as build, scb.job_name, 
		scb.row_count as beforeRowCount, sca.row_count as afterRowCount, scb.seconds_query_duration as beforeDur, sca.seconds_query_duration as afterDur,
		sca.row_count - scb.row_count as rawDiff, FLOOR(LN(scb.row_count)) as logSrcCount,
		CASE WHEN sca.row_count IS NULL THEN scb.row_count
			WHEN scb.row_count <= sca.row_count THEN scb.row_count
			ELSE sca.row_count
		END as lowSrcCount,
		CASE WHEN sca.row_count IS NULL THEN scb.row_count
			WHEN scb.row_count >= sca.row_count THEN scb.row_count
			ELSE sca.row_count
		END as highSrcCount
	FROM cteSourceCounts scb
	LEFT JOIN cteSourceCounts sca ON sca.project_id = scb.project_id AND sca.source_schema = scb.source_schema AND sca.table_name = scb.table_name AND sca.job_name = scb.job_name AND sca.is_after = 't'
	WHERE COALESCE(scb.is_after, 'f') = 'f' 
)
--SELECT * FROM cteAmalgSourceCounts
SELECT c.schema_name, c.table_name, ROUND(c.schema_create_time, 'SS') AS schema_create_time, c.row_count as vertRowCount, src.lowSrcCount, src.highSrcCount, 
	ROUND(src.beforeQueryTime, 'SS') as srcBefQueryTime, src.beforeDur AS befDurSecs, logSrcCount * 3 as tolDiff,
	CASE WHEN COALESCE(c.row_count, 0) < src.lowSrcCount THEN COALESCE(c.row_count, 0) - src.lowSrcCount
		WHEN COALESCE(c.row_count, 0) > src.highSrcCount THEN COALESCE(c.row_count, 0) - src.highSrcCount
		ELSE NULL::INT
	END as boundDiff,
	CAST(c.PopdTblsCount AS VARCHAR(10))  || ' of ' || CAST(TableCnt AS VARCHAR(10)) as vertPopdTbls, src.build,
	CASE WHEN src.lowSrcCount IS NULL OR c.row_count BETWEEN src.lowSrcCount AND src.highSrcCount THEN '.'
		-- arbitrary decision to declare any diff, vs. low count from source, <= logSrcCount * 3 to be "bad but not that bad"..
		WHEN ABS(COALESCE(c.row_count, 0) - src.lowSrcCount) <= logSrcCount * 3 OR ABS(COALESCE(c.row_count, 0) - src.highSrcCount) <= logSrcCount * 3 THEN '>>>> ooh, its close <<<<'
		ELSE '>>>> WHOOPS row count <<<<' 
	END AS RowCountCheck
	-- debug stuff
	--,logSrcCount, src.beforeDur, src.afterDur, src.beforeRowCount, src.afterRowCount
FROM cteCombined c
LEFT JOIN cteAmalgSourceCounts src ON src.project_id = c.schema_name AND src.table_name = c.table_name
WHERE 1 = 1
ORDER BY c.schema_name, c.table_name

