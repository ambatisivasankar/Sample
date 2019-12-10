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

# Need single quotes for this json object because double quotes will not epand variables
# $strt_dt and $end_dt are set in Jenkins at execution time
export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'AGMT_FIN_TXN_CMN_VW': {
            'sql_query': '(SELECT * FROM ST_A_USIG_STND_VW.AGMT_FIN_TXN_CMN_VW where TRANS_EFFECTIVE_DATE  between cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date) as subquery',
            'numPartitions': 10,
            'partitionColumn': '(AGREEMENT_ID Mod 10)',
            'lowerBound': 0,
            'upperBound': 10
        }
    }
}
"
