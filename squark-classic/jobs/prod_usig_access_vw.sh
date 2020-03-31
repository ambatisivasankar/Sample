export PROJECT_ID=prod_usig_access_vw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata_access
export SPARK_MAX_EXECUTORS=5

include_tables_array=(
  "AFT_WHLSLR_HIER_VW"
  "SALES_HIERARCHY_VW"
)

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables


export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'SALES_HIERARCHY_VW': {
              'partitionColumn': 'AGY_PRTY_ID MOD 5',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            }
        }
   }
}
"
