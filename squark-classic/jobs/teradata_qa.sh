export PROJECT_ID=teradata_qa
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_CMN_VERTICA_VW,AGMT_VAL_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,SLLNG_AGMT_CMN_VW,AGMT_UWRT_CMN_VW,AGMT_ADDL_DATA_VW,AGMT_CVG_CMN_VW,AGMT_FND_CMN_VW,AGMT_FND_VAL_CMN_VW,AGMT_LOAN_CMN_VW,BENE_DATA_CMN_VW,FUND_CMN_VW'
export CONNECTION_ID=teradata_qa
export SPARK_YARN_QUEUE='datalayer'
# expected at least 2 _qa.sh jobs + the _prty_qa.sh (same id, diff db) job will be running simultaneously, limit connections
export SPARK_MAX_EXECUTORS=10

# need to occasionally monitor CUST_DEMOGRAPHICS_VW for further growth in PRTY_ID values
# 2018.02.07 FND_ID = 425115 in FUND_CMN_VW does NOT want to write to S3, skipping via subquery
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_CMN_VERTICA_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 50
            },
            'AGMT_VAL_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 50
            },
            'PDCR_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 50
            },
            'CUST_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 30000000,
              'numPartitions': 50
            },
            'CUST_DEMOGRAPHICS_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 21800000,
              'upperBound': 33000000,
              'numPartitions': 50
            },
            'FUND_CMN_VW': {
              'partitionColumn': 'FND_ID NOT IN (425115) AND FND_ID',
              'lowerBound': 100000,
              'upperBound': 470000,
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
