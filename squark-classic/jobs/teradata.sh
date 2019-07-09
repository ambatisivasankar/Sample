export PROJECT_ID=teradata
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata
export SPARK_MAX_EXECUTORS=15

INCLUDE_TABLES_ARRAY=(
  "AGMT_CMN_VW"
  "AGMT_CVG_CMN_VW"
  "AGMT_DISB_TXN_CMN_VW"
  "AGMT_UWRT_CMN_VW"
  "AGMT_VAL_CMN_VW"
  "BENE_DATA_CMN_VW"
  "BENE_DATA_HIST_VW"
  "BP_CREDENTIAL_VW"
  "CUST_ADDL_AD_CMN_VW"
  "CUST_AGMT_CMN_VW"
  "CUST_DEMOGRAPHICS_VW"
  "CUST_PREFERENCE_VW"
  "PDCR_AGMT_CMN_VW"
  "PDCR_AGMT_HIST_VW"
  "PDCR_ALT_ID_CMN_VW"
  "PDCR_DEMOGRAPHICS_HIST_VW"
  "PDCR_DEMOGRAPHICS_VW"
  "SLLNG_AGMT_CMN_VW"
  )

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 0,
              'upperBound': 52000000,
              'numPartitions': 50
            },
            'AGMT_CVG_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5500000,
              'upperBound': 52000000,
              'numPartitions': 50
            },
            'AGMT_UWRT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5600000,
              'upperBound': 52000000,
              'numPartitions': 15
            },
            'AGMT_VAL_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 0,
              'upperBound': 52000000,
              'numPartitions': 50
            },
            'BENE_DATA_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5500000,
              'upperBound': 52000000,
              'numPartitions': 50
            },
            'BENE_DATA_HIST_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 5500000,
              'upperBound': 52000000,
              'numPartitions': 50
            },
            'BP_CREDENTIAL_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 8500000,
              'upperBound': 9000000,
              'numPartitions': 5
            },
            'CUST_ADDL_AD_CMN_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 1000000,
              'upperBound': 32000000,
              'numPartitions': 25
            },
            'CUST_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 0,
              'upperBound': 52000000,
              'numPartitions': 100
            },
            'CUST_DEMOGRAPHICS_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 0,
              'upperBound': 33000000,
              'numPartitions': 50
            },
            'PDCR_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 0,
              'upperBound': 52000000,
              'numPartitions': 100
            },
            'PDCR_AGMT_HIST_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 0,
              'upperBound': 52000000,
              'numPartitions': 100
            },
            'PDCR_ALT_ID_CMN_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 8500000,
              'upperBound': 9000000,
              'numPartitions': 5
            },
            'PDCR_DEMOGRAPHICS_HIST_VW': {
              'partitionColumn': 'PRTY_ID',
              'lowerBound': 8500000,
              'upperBound': 9000000,
              'numPartitions': 5
            }
        }
   }
}
"
