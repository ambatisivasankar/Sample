# Required
export PROJECT_ID=teradata_finhist_st
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=teradata_st

# Optional
export INCLUDE_VIEWS=1
export SPARK_MAX_EXECUTORS=5

include_tables_array=(
  "AGMT_FIN_TXN_CMN_VW"
)


include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

# IDL = Initial Date Load 0 = False, 1 = True (set in jenkins, defaults to False if not set)
IDL=${IDL:-0}

# BACKFILL = 0 = False, 1 = True (set in jenkins, defaults to False if not set)
BACKFILL=${BACKFILL:-0}

# Need single quotes for this json object because double quotes will not epand variables
# $strt_dt and $end_dt are set in Jenkins at execution time
if [ "${IDL}" -eq 1 ]; then
  echo "Setting JSON_INFO for initial data load"
  export JSON_INFO="
  {
      'SAVE_TABLE_SQL_SUBQUERY':{
          'AGMT_FIN_TXN_CMN_VW': {
              'sql_query': '(SELECT * FROM ST_A_USIG_STND_VW.AGMT_FIN_TXN_CMN_VW where TRANS_EFFECTIVE_DATE  between cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
              'numPartitions': 10,
              'partitionColumn': '(AGREEMENT_ID Mod 10)',
              'lowerBound': 0,
              'upperBound': 10
          }
      },
      'TABLE_MAP':{
          'AGMT_FIN_TXN_CMN_VW': 'AGMT_FIN_TXN_CMN_VW_st'
      }
  }
  "
elif [ "${BACKFILL}" -eq 1 ]; then
  echo "Setting JSON_INFO for backfill"
  export JSON_INFO="
  {
      'SAVE_TABLE_SQL_SUBQUERY':{
          'AGMT_FIN_TXN_CMN_VW': {
              'sql_query': '(SELECT * FROM ST_A_USIG_STND_VW.AGMT_FIN_TXN_CMN_VW where TRANS_EFFECTIVE_DATE in (${DATES})) as subquery',
              'numPartitions': 10,
              'partitionColumn': '(AGREEMENT_ID Mod 10)',
              'lowerBound': 0,
              'upperBound': 10
          }
      },
      'TABLE_MAP':{
          'AGMT_FIN_TXN_CMN_VW': 'AGMT_FIN_TXN_CMN_VW_st'
      }
  }
  "
else
  echo "Setting JSON_INFO for incremental load"

  # DELTA_RANGE = Number of Dates to use for Incremetal/Delta query. (set in jenkins, defaults to 7 if not set)
  DELTA_RANGE=${DELTA_RANGE:-7}

  # PARTS = Number of partitions to use for Incremantal/Delta query (set in jenkins, defaults to 2 if not set)
  PARTS=${PARTS:-2}
  # If end date is not set then use current date as end date
  current_date=$(date '+%Y-%m-%d')
  export end_dt=${end_dt:-${current_date}}
  echo "$end_dt=${current_date}"
  export JSON_INFO="
  {
      'SAVE_TABLE_SQL_SUBQUERY':{
          'AGMT_FIN_TXN_CMN_VW': {
              'sql_query': '(SELECT * FROM ST_A_USIG_STND_VW.AGMT_FIN_TXN_CMN_VW where TRANS_DT  between cast('''$end_dt''' as date) - $DELTA_RANGE AND cast('''$end_dt''' as date)) as subquery',
              'numPartitions': $PARTS,
              'partitionColumn': '(AGREEMENT_ID Mod $PARTS)',
              'lowerBound': 0,
              'upperBound': $PARTS
          }
      },
      'TABLE_MAP':{
          'AGMT_FIN_TXN_CMN_VW': 'AGMT_FIN_TXN_CMN_VW_st'
      }
  }
  "
fi
