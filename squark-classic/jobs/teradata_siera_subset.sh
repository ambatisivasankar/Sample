# Required
export PROJECT_ID=teradata_siera
export CONNECTION_ID=prod_usig_crcog_dm_rptg_vw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export SPARK_MAX_EXECUTORS=6

include_tables_array=(
  "CAL_VW"
  "SLS_RPTG_PREM_HIST_VW"
)

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'CAL_VW': {
            'sql_query': '(SELECT * FROM PROD_USIG_CRCOG_DM_RPTG_VW.CAL_VW) as subquery'
        },
        'SLS_RPTG_PREM_HIST_VW': {
            'sql_query': '(SELECT * FROM PROD_USIG_CRCOG_DM_RPTG_VW.SLS_RPTG_PREM_HIST_VW) as subquery',
            'numPartitions': 13,
            'partitionColumn': 'COALESCE(EXTRACT(MONTH FROM CYCLE_DT), 13)',
            'lowerBound': 0,
            'upperBound': 13
        }
    },
      'TABLE_MAP':{
          'CAL_VW': 'teradata_siera_cal_vw',
          'SLS_RPTG_PREM_HIST_VW': 'teradata_siera_sls_rptg_prem_hist_vw'
      }
}
"
