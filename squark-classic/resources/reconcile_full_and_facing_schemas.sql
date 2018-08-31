WITH cteTables AS(
	SELECT table_name
	FROM tables
	WHERE table_schema = :TABLE_LIST_SCHEMA
)
,cteFull AS (
	SELECT 'SELECT ''' || table_name || ''' as table_name, ''1. _advana_md5 in FULL_SCHEMA (' || :FULL_SCHEMA || ') but not FACING_SCHEMA (' || :FACING_SCHEMA || ')'' as ingest_type, ''...checking... (LIMIT 10)'' as status;' as query
	FROM cteTables
    UNION ALL
	SELECT 'SELECT ''' || table_name || ''' as table_name, ''2. ALERT--ALERT _advana_md5 in FULL_SCHEMA (' || :FULL_SCHEMA || ') but not FACING_SCHEMA (' || :FACING_SCHEMA || ')'' as ingest_type, _advana_md5 FROM ' || :FULL_SCHEMA || '.' || table_name ||
		' WHERE _advana_md5 NOT IN (SELECT _advana_md5 FROM '  || :FACING_SCHEMA || '.' || table_name || ') LIMIT 10;' 	as query
	FROM cteTables
)
--SELECT * FROM cteFull
,cteFacing AS (
	SELECT 'SELECT ''' || table_name || ''' as table_name, ''3. _advana_md5 in FACING_SCHEMA (' || :FACING_SCHEMA || ') but not FULL_SCHEMA (' || :FULL_SCHEMA || ')'' as ingest_type, ''...checking... (LIMIT 10)'' as status;' as query
	FROM cteTables
    UNION ALL
	SELECT 'SELECT ''' || table_name || ''' as table_name, ''4. ALERT--ALERT _advana_md5 in FACING_SCHEMA (' || :FACING_SCHEMA || ') but not FULL_SCHEMA (' || :FULL_SCHEMA || ')'' as ingest_type, _advana_md5 FROM ' || :FACING_SCHEMA || '.' || table_name ||
		' WHERE _advana_md5 NOT IN (SELECT _advana_md5 FROM '  || :FULL_SCHEMA || '.' || table_name || ') LIMIT 10;' as query
	FROM cteTables
)
SELECT * FROM cteFull
UNION ALL
SELECT * FROM cteFacing
ORDER BY query;
