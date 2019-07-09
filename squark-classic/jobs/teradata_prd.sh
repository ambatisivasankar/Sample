export PROJECT_ID=teradata_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='BP_CREDENTIAL_VW,AGMT_CMN_VW,AGMT_VAL_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,SLLNG_AGMT_CMN_VW,AGMT_ADDL_DATA_VW,AGMT_CVG_CMN_VW,AGMT_FND_CMN_VW,AGMT_FND_VAL_CMN_VW,AGMT_LOAN_CMN_VW,AGMT_UWRT_CMN_VW,BENE_DATA_CMN_VW,FUND_CMN_VW,AGMT_GRP_CMN_VW,AGMT_WARNING_INFO_CMN_VW'
# new teradata_prd schema will still pull from standard teradata source
export CONNECTION_ID=teradata
export SPARK_YARN_QUEUE='datalayer'
# expected at least 2 _prd.sh jobs + the _prty_prd.sh (same id, diff db) job will be running simultaneously, limit connections
export SPARK_MAX_EXECUTORS=10
export CONVERT_TIMESTAMPS_TO_AMERICA_NEW_YORK=1
export MAKE_DDL_FROM_TARGET=1

# need to occasionally monitor CUST_DEMOGRAPHICS_VW for further growth in PRTY_ID values
# 2018.02.07 FND_ID = 425115 in FUND_CMN_VW does NOT want to write to S3, skipping via subquery
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
            'AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 35000000,
              'numPartitions': 50
            },
            'AGMT_VAL_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 35000000,
              'numPartitions': 50
            },
            'PDCR_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 35000000,
              'numPartitions': 50
            },
            'CUST_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 35000000,
              'numPartitions': 50
            },
            'CUST_DEMOGRAPHICS_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 1,
              'upperBound': 22000000,
              'numPartitions': 50
            },
            'AGMT_ADDL_DATA_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 6000000,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'AGMT_CVG_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'AGMT_FND_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'AGMT_FND_VAL_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'AGMT_LOAN_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 500,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'AGMT_UWRT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'BENE_DATA_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
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
