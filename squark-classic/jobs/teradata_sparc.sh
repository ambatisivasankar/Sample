export PROJECT_ID=teradata_sparc
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='COMP_VW'
export CONNECTION_ID=teradata_sparc

export SPARK_MAX_EXECUTORS=15
#AGMT_ID is technically nullable but populated for all rows as of July 2017 and has best distribution for partitioning purposes
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'COMP_VW': {
              'partitionColumn': 'COALESCE(AGMT_ID,0)',
              'lowerBound': 1,
              'upperBound': 35000000,
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
