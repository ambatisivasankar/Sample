#!/usr/bin/env bash

# quick and dirty to have available in a shell file
# env variables that need to have been set: STAT_SCHEMA_NAME, $STAT_VERTICA_HOST, $STAT_VERTICA_USER, $STAT_VERTICA_PASSWORD
analyze_statistics_sql="analyze_statistics.sql"

stats_sql=$(cat <<-EOM
SELECT 'SELECT ''' || t.table_schema || '.' || t.table_name || ''' as tbl,
    ANALYZE_STATISTICS (''' || t.table_schema || '.' || t.table_name || ''');'
FROM tables t
WHERE table_schema = '$STAT_SCHEMA_NAME';
EOM
)

# 1) create the statements to execute, e.g.   SELECT ANALYZE_STATISTICS ('haven_daily.address');
vsql -h $STAT_VERTICA_HOST -U $STAT_VERTICA_USER -w $STAT_VERTICA_PASSWORD -d advana -c "$stats_sql" -o $analyze_statistics_sql -At

cat $analyze_statistics_sql

# 2) execute all of the ANALYZE_STATISTICS statements
vsql -h $STAT_VERTICA_HOST -U $STAT_VERTICA_USER -w $STAT_VERTICA_PASSWORD -d advana -f $analyze_statistics_sql

