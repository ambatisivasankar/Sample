# Required
export PROJECT_ID=sfmc
export CONNECTION_ID=teradata_qa
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=0
export SKIP_SOURCE_ROW_COUNT=0
export SPARK_MAX_EXECUTORS=5
echo "Start dt: " $strt_dt
echo "End Dt: " $end_dt

# SF_MM_ACCOUNT does not exist in the CONNECTION_ID used for QA
# we use FAUX_CVG_ID_HIST_VW as an 'alias' table in Vertica QA
include_tables_array=(
  "FAUX_CVG_ID_HIST_VW"
  "SF_MM_ACCOUNT"
)

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'FAUX_CVG_ID_HIST_VW': {
            'sql_query': '(SELECT * FROM QA_ESP.SF_MM_ACCOUNT) as subquery',
            'numPartitions': 60,
            'partitionColumn': 'EXTRACT(SECOND FROM LastModifiedDate_UTC)',
            'lowerBound': 0,
            'upperBound': 59
        }
    }
}
"
