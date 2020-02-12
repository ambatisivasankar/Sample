export PROJECT_ID=prod_stnd_prty1
# primary purpose of schema is to refresh select schemas on a daily basis
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='PDCR_DEMOGRAPHICS_PHV'
export CONNECTION_ID=teradata

export SPARK_MAX_EXECUTORS=15

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'PDCR_DEMOGRAPHICS_PHV': {
              'partitionColumn': 'COALESCE(PRTY_ID,0)',
              'lowerBound': 8472126,
              'upperBound': 57236804,
              'numPartitions': 100
            }
        }
   }
}
"
