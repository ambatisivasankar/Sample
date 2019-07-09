export PROJECT_ID=teradata_st
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='BP_CREDENTIAL_VW,AGMT_CMN_VERTICA_VW,AGMT_VAL_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,SLLNG_AGMT_CMN_VW,AGMT_ADDL_DATA_VW,AGMT_CVG_CMN_VW,AGMT_FND_CMN_VW,AGMT_FND_VAL_CMN_VW,AGMT_LOAN_CMN_VW,FUND_CMN_VW,AGMT_UWRT_CMN_VW,BENE_DATA_CMN_VW,AGMT_GRP_CMN_VW,AGMT_WARNING_INFO_CMN_VW'
export CONNECTION_ID=teradata_st
export SPARK_MAX_EXECUTORS=5
export CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK=1

# set numPartitions to 200 for HDFS optimization, to 50 for AWS optimization
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
          'BP_CREDENTIAL_VW': {
            'partitionColumn': 'PRTY_ID',
            'lowerBound': 8000000,
            'upperBound': 35000000,
            'numPartitions': 50
          },
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
            'FUND_CMN_VW': {
              'partitionColumn': 'FND_ID NOT IN (425115) AND FND_ID',
              'lowerBound': 100000,
              'upperBound': 700000,
              'numPartitions': 50
            },
            'AGMT_CVG_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
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
