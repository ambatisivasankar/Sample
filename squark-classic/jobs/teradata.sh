export PROJECT_ID=teradata
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,AGMT_UWRT_CMN_VW,PDCR_ALT_ID_CMN_VW,CUST_PREFERENCE_VW,SLLNG_AGMT_CMN_VW,AGMT_VAL_CMN_VW,AGMT_CVG_CMN_VW,BP_CREDENTIAL_VW,AGMT_DISB_TXN_CMN_VW,BENE_DATA_CMN_VW,BENE_DATA_HIST_VW'
export CONNECTION_ID=teradata

export SPARK_MAX_EXECUTORS=15

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
            },
            'BENE_DATA_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 39000000,
              'numPartitions': 50
            },
            'BENE_DATA_HIST_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5700000,
              'upperBound': 49000000,
              'numPartitions': 50
            }
        }
   }
}
"
