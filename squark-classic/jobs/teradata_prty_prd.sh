export PROJECT_ID=teradata_prty_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='PRTY_ALT_ID_VW'
#teradata_prty_prd schema pulls from standard prty data source
export CONNECTION_ID=teradata_prty
export SPARK_YARN_QUEUE='datalayer'
export SPARK_MAX_EXECUTORS=10

#2017.04.04, curr PRTY_ID distribution is bimodal and many of the partitioned queries will not return any results
# seeing about 130 .orc instead of potential 200, but still quicker vs. EXTRACT(MINUTE FROM PRTY_DATA_FR_DT)
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'PRTY_ALT_ID_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 1,
              'upperBound': 22000000,
              'numPartitions': 50
            }
        }
    }
}
"
