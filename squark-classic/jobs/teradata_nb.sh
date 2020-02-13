export PROJECT_ID=teradata_nb
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata_nb

export SPARK_MAX_EXECUTORS=5

include_tables_array=(
  "DI_NB_RPT_VW"
  "INSURED_VW"
  "NB_APPL_PRTY_VW"
  "NB_APPL_VW"
  "NB_BILL_INFO_VW"
  "NB_BLDED_SUBMIT_APPL_DTL_VW"
  "NB_COV_RISK_VW"
  "NB_PRTY_APPL_AD_VW"
  "NB_PRTY_APPL_RLE_VW"
  "NB_PRTY_CASE_OWN_VW"
  "NB_RIDER_INFO_VW"
  "NB_RPT_VW"
)

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'INSURED_VW': {
              'partitionColumn': 'CASE_ID MOD 5',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            },
            'NB_APPL_PRTY_VW': {
              'partitionColumn': 'PRTY_ID MOD 5',
              'lowerBound': 0,
              'upperBound': 10,
              'numPartitions': 10
            },
            'NB_APPL_VW': {
              'partitionColumn': 'APPL_ID MOD 50',
              'lowerBound': 0,
              'upperBound': 50,
              'numPartitions': 50
            },
            'NB_BILL_INFO_VW': {
              'partitionColumn': 'APPL_ID MOD 25',
              'lowerBound': 0,
              'upperBound': 25,
              'numPartitions': 25
            },
            'NB_BLDED_SUBMIT_APPL_DTL_VW': {
              'partitionColumn': 'MRG_AGENCY_SRC_SYS_PRTY_ID MOD 5',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            },
            'NB_COV_RISK_VW': {
              'partitionColumn': 'APPL_ID MOD 10',
              'lowerBound': 0,
              'upperBound': 10,
              'numPartitions': 10
            },
            'NB_PRTY_APPL_AD_VW': {
              'partitionColumn': 'PRTY_APL_AD_ID MOD 10',
              'lowerBound': 0,
              'upperBound': 10,
              'numPartitions': 10
            },
            'NB_PRTY_APPL_RLE_VW': {
              'partitionColumn': 'APPL_ID MOD 100',
              'lowerBound': 0,
              'upperBound': 100,
              'numPartitions': 100
            },
            'NB_PRTY_CASE_OWN_VW': {
              'partitionColumn': 'APPL_ID MOD 50',
              'lowerBound': 0,
              'upperBound': 50,
              'numPartitions': 50
            },
            'NB_RIDER_INFO_VW': {
              'partitionColumn': 'APPL_ID MOD 10',
              'lowerBound': 0,
              'upperBound': 10,
              'numPartitions': 10
            },
            'NB_RPT_VW': {
              'partitionColumn': 'APPL_ID MOD 10',
              'lowerBound': 0,
              'upperBound': 10,
              'numPartitions': 10
            }
        }
   }
}
"
