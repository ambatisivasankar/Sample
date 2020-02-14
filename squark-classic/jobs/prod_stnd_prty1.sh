export PROJECT_ID=prod_stnd_prty1
# primary purpose of schema is to refresh select schemas on a daily basis
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='PDCR_DEMOGRAPHICS_VW'
export CONNECTION_ID=teradata

export SPARK_MAX_EXECUTORS=10

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'PDCR_DEMOGRAPHICS_VW': {
              'partitionColumn': 'PRTY_ID mod 10',
              'lowerBound': 0,
              'upperBound': 9,
              'numPartitions': 10
            }
        }
   }
}
"
