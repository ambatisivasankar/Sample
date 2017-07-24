export PROJECT_ID=teradata_prd
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_CMN_VW,AGMT_VAL_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,SLLNG_AGMT_CMN_VW'
# new teradata_prd schema will still pull from standard teradata source
export CONNECTION_ID=teradata
export SPARK_YARN_QUEUE='datalayer'
# expected at least 2 _prd.sh jobs + the _prty_prd.sh (same id, diff db) job will be running simultaneously, limit connections
export SPARK_MAX_EXECUTORS=10

# need to occasionally monitor CUST_DEMOGRAPHICS_VW for further growth in PRTY_ID values
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
            'AGMT_VAL_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
            'PDCR_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
            'CUST_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
            'CUST_DEMOGRAPHICS_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 1,
              'upperBound': 20000000,
              'numPartitions': 50
            }
        }
   }
}
"
