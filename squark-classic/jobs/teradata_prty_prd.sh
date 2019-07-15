# Required
export PROJECT_ID=teradata_prty_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='PRTY_ALT_ID_VW'
# teradata_prty_prd schema pulls from standard teradata_prty data source
export CONNECTION_ID=teradata_prty
export SPARK_YARN_QUEUE='datalayer'
export SPARK_MAX_EXECUTORS=10

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'PRTY_ALT_ID_VW': {
              'partitionColumn': 'PRTY_ID MOD 50',
              'lowerBound': 0,
              'upperBound': 50,
              'numPartitions': 50
            }
        }
    }
}
"
