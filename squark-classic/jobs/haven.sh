# Dummy file required by load_wh.sh
export PROJECT_ID=haven
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven
export SPARK_MAX_EXECUTORS=30
export SQUARK_METADATA=1

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'analytics_event': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 60,
              'numPartitions': 60
            },
            'interaction': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 60,
              'numPartitions': 60
            },
            'policy_doc': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 60,
              'numPartitions': 60
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
