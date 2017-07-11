export PROJECT_ID=teradata_qa
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_CMN_VERTICA_VW,AGMT_VAL_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,SLLNG_AGMT_CMN_VW'
export CONNECTION_ID=teradata_qa
export SPARK_YARN_QUEUE='datalayer'
# expected at least 2 _qa.sh jobs + the _prty_qa.sh (same id, diff db) job will be running simultaneously, limit connections
export SPARK_MAX_EXECUTORS=10

# need to occasionally monitor CUST_DEMOGRAPHICS_VW for further growth in PRTY_ID values
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_CMN_VERTICA_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 200
            },
            'AGMT_VAL_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 200
            },
            'PDCR_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 200
            },
            'CUST_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 200
            },
            'CUST_DEMOGRAPHICS_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 21800000,
              'upperBound': 33000000,
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
