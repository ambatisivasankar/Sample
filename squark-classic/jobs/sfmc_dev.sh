# Required
export PROJECT_ID=sfmc
export CONNECTION_ID=drc_dev
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=0
export SKIP_SOURCE_ROW_COUNT=0
export SPARK_MAX_EXECUTORS=5
echo "Start dt: " $strt_dt
echo "End Dt: " $end_dt

# SF_MM_ACCOUNT does not exist in the CONNECTION_ID used for Dev
# we use BCC_QUAL_CD_VW as an 'alias' table in Vertica Dev
include_tables_array=(
  "BCC_QUAL_CD_VW"
  "SF_MM_ACCOUNT"
)

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'BCC_QUAL_CD_VW': {
            'sql_query': '(SELECT * FROM DEV_ESP.SF_MM_ACCOUNT) as subquery',
            'numPartitions': 60,
            'partitionColumn': 'EXTRACT(SECOND FROM LastModifiedDate_UTC)',
            'lowerBound': 0,
            'upperBound': 59
        }
    }
}
"
