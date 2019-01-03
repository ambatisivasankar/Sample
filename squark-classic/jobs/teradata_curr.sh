export PROJECT_ID=teradata_curr
# primary purpose of schema is to refresh select schemas on a daily basis
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_COMM_TXN_CMN_VW'
export CONNECTION_ID=teradata

export SPARK_MAX_EXECUTORS=15

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_COMM_TXN_CMN_VW': {
              'partitionColumn': 'COALESCE(RUN_ID,0)',
              'lowerBound': 0,
              'upperBound': 800,
              'numPartitions': 80
            }
        }
   }
}
"
