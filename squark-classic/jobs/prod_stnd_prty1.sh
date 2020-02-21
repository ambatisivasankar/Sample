export PROJECT_ID=prod_stnd_prty1
# primary purpose of schema is to refresh select schemas on a daily basis
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata_prod_stnd_prty

INCLUDE_TABLES_ARRAY=(
  "BP_DNP_HIST"
  "CUST_AGMT_CMN_PHV"
  "CUST_DEMOGRAPHICS_PHV"
  "PDCR_DEMOGRAPHICS_PHV"
  "PRTY"
  "PRTY_AD"
  "PRTY_ALT_ID"
  "PRTY_SLLNG_AGMT"
  "SLLNG_AGMT"
  "SLLNG_AGMT_CMN_PHV"
  "SLLNG_AGMT_DTL"
  "SLLNG_AGMT_SM_SPLITS"
  "DTCHD_OFC"
)

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"
export SPARK_MAX_EXECUTORS=10

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'PDCR_DEMOGRAPHICS_PHV': {
              'partitionColumn': 'PRTY_ID mod 10',
              'lowerBound': 0,
              'upperBound': 9,
              'numPartitions': 10
            },
             'CUST_AGMT_CMN_PHV': {
              'partitionColumn': 'oreplace((prty_id || agreement_id),' ','') mod 10',
              'lowerBound': 0,
              'upperBound': 9,
              'numPartitions': 10
            },
             "CUST_DEMOGRAPHICS_PHV": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "PRTY": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "PRTY_AD": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "PRTY_ALT_ID": {
              "partitionColumn": "PRTY_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "PRTY_SLLNG_AGMT": {
              "partitionColumn": "CONTR_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "SLLNG_AGMT": {
              "partitionColumn": "CONTR_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "SLLNG_AGMT_CMN_PHV": {
              "partitionColumn": "CONTR_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            },
            "SLLNG_AGMT_DTL": {
              "partitionColumn": "CONTR_ID MOD 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "SLLNG_AGMT_SM_SPLITS": {
              "partitionColumn": "CONTR_ID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10 
           },
           'DTCHD_OFC': {
              'partitionColumn': 'AGY_PRTY_ID mod 10',
              'lowerBound': 0,
              'upperBound': 9,
              'numPartitions': 10
            },
           "BP_DNP_HIST": {
              "partitionColumn": "BP_DNP_HIST_SID MOD 10",
              "lowerBound": 0,
              "upperBound": 10,
              "numPartitions": 10
            }
       }
   }
}
"
