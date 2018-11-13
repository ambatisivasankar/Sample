export PROJECT_ID=drc
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=drc

export SPARK_EXECUTOR_MEMORY="4G"
export SPARK_MAX_EXECUTORS=15

export EXCLUDE_TABLES_ARRAY=(
    "BI_NFF_FACT_VW"
    "BI_SLS_DISB_FACT_VW"
    "TIER_NR"
)
export INCLUDE_TABLES_ARRAY=(
    "CAL_VW"
    "ADVSR_VW"
    "AGCY_AFFLTN_VW"
)

export EXCLUDE_TABLES="$(IFS=, ; echo "${EXCLUDE_TABLES_ARRAY[*]}")"
export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

# 2018.11.13, cutting down to only three tables, none of which are covered below, keeping around just in case
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'SLS_AND_DTRB_FACT_VW': {
              'partitionColumn': 'SLS_DTRB_SID',
              'lowerBound':  80000000,
              'upperBound': 300000000,
              'numPartitions': 50
            },
            'NFF_FACT_VW': {
              'partitionColumn': 'NFF_SID',
              'lowerBound': 1,
              'upperBound': 10000000,
              'numPartitions': 50
            }
        }
    }
}
"
