#!/usr/bin/env bash

# quick and dirty to have available in a shell file
# if executing in same shell as jenkins job would only need to set STAT_SCHEMA_NAME
# as a standalone jenkins job, assuming global pwds available, something like below
#   source "./aws-env-cluster.sh"
#   export AWS_VERTICA_HOST=$AWS_VERTICA_QA
#   export AWS_VERTICA_PORT=5433
#   export STAT_SCHEMA_NAME="haven_weekly"
#   ./squark-classic/analyze_statistics_standalone.sh

analyze_statistics_sql="analyze_statistics.sql"

stats_sql=$(cat <<-EOM
SELECT 'SELECT ''' || t.table_schema || '.' || t.table_name || ''' as tbl, ANALYZE_STATISTICS (''' || t.table_schema || '.' || t.table_name || ''');'
FROM tables t
WHERE table_schema = '$STAT_SCHEMA_NAME';
EOM
)

# 1) create the statements to execute, e.g.   SELECT ANALYZE_STATISTICS ('haven_daily.address');
$VERTICA_VSQL -h $AWS_VERTICA_HOST -U $VERTICA_USER -w $AWS_VERTICA_PASSWORD -d $VERTICA_DATABASE -c "$stats_sql" -o $analyze_statistics_sql -At

echo ">> list contents of "$analyze_statistics_sql
cat $analyze_statistics_sql

echo ">> execute contents of "$analyze_statistics_sql
# 2) execute all of the ANALYZE_STATISTICS statements
$VERTICA_VSQL  -h $AWS_VERTICA_HOST -U $VERTICA_USER -w $AWS_VERTICA_PASSWORD -d $VERTICA_DATABASE -f $analyze_statistics_sql

