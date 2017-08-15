export PROJECT_ID=esp
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=esp
export EXCLUDE_TABLES="ANLTCS_NEXT_PURCHASE"

export SPARK_MAX_EXECUTORS=15
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'CATS_WRK_TXT_CMNT': {
              'partitionColumn': 'FK_WRK_IDENT',
              'lowerBound': 5000000000,
              'upperBound': 5092000000,
              'numPartitions': 50
            },
            'CATS_WRK': {
              'partitionColumn': 'WRK_IDENT',
              'lowerBound': 5000000000,
              'upperBound': 5092000000,
              'numPartitions': 50
            },
            'CSTM_PDCR_RPT': {
              'partitionColumn': 'MMID',
              'lowerBound': 0,
              'upperBound': 620000,
              'numPartitions': 50
            },
            'SF_MM_ACCOUNT': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_HOLDING': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM CreatedDate), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_ACCOUNT_pre1Updates': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_ACCOUNT_20170118': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_ACCOUNT_20160927': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
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
