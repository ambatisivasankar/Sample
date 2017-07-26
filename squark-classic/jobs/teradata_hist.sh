export PROJECT_ID=teradata_hist
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='CUST_AGMT_HIST_VW,CUST_DEMOGRAPHICS_HIST_VW,AGMT_CVG_HIST_VW'
export CONNECTION_ID=teradata

export SPARK_MAX_EXECUTORS=15
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'CUST_AGMT_HIST_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
            'CUST_DEMOGRAPHICS_HIST_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 1,
              'upperBound': 22000000,
              'numPartitions': 50
            },
            'AGMT_CVG_HIST_VW': {
              'partitionColumn': 'COALESCE(AGREEMENT_ID, 0)',
              'lowerBound': 5600000,
              'upperBound': 34000000,
              'numPartitions': 50
            }
        }
   }
}
"
#export STATS_CONFIG="
#{
#    'profiles': {
#        'numeric': ['max','min','mean','countDistinct','count_null'],
#        'string': ['max', 'min','countDistinct','count_null'],
#        'datetype': ['max', 'min','countDistinct','count_null']
#    },
#    'field_types': {
#        'NumericType': 'numeric',
#        'StringType': 'string',
#        'DateType': 'datetype',
#        'TimestampType': 'datetype'
#    }
#}
#"
