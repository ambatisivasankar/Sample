export PROJECT_ID=teradata_prty
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES=PRTY_ALT_ID_VW
export CONNECTION_ID=teradata_prty
export SPARK_MAX_EXECUTORS=10

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'PRTY_ALT_ID_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 1,
              'upperBound': 17500000,
              'numPartitions': 200
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
