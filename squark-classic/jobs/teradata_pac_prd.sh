# Required
export PROJECT_ID=teradata_pac_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_PAC_DATA_VW'
export CONNECTION_ID=teradata_pac_prd
export SPARK_MAX_EXECUTORS=2
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_PAC_DATA_VW': {
              'partitionColumn': 'AGMT_ID MOD 10',
              'lowerBound': 0,
              'upperBound': 10,
              'numPartitions': 10
            }
        }
    }
}
"
