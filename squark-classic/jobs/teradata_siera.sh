# Required
export PROJECT_ID=teradata_siera
export CONNECTION_ID=drc_sierra
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=0
export SPARK_MAX_EXECUTORS=6
# export SPARK_EXECUTOR_MEMORY="4G"
echo "Start dt: " $strt_dt
echo "End Dt: " $end_dt

include_tables_array=(
  "ADVSR"
  "AGMT"
  "AGMT_COMM_TXN"
  "COMM_FACT"
  "PREMIUM_FACT"
  "PROD"
)

include_tables="$(IFS=, ; echo "${include_tables_array[*]}")"
export INCLUDE_TABLES=$include_tables

#   in order to use value in the subfilter, double-quote all innards of the JSON
export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'ADVSR': {
            'sql_query': '(SELECT DISTINCT ADVSR_SID,LST_MOD_TS,BP_ID FROM PROD_USIG_CRCOG_DM.ADVSR) as subquery',
            'numPartitions': 11,
            'partitionColummn': '(ADVSR_SID MOD 10)',
            'lowerBound': -1,
            'upperBound': 9
        },
        'AGMT': {
            'sql_query': '(SELECT DISTINCT AGMT_SID,LST_MOD_TS,HLDG_KEY_PFX,HLDG_KEY,HLDG_KEY_SFX,CARR_ADMIN_SYS_CD FROM PROD_USIG_CRCOG_DM.AGMT) as subquery',
            'numPartitions': 20,
            'partitionColummn': '(AGMT_SID MOD 20)',
            'lowerBound': 0,
            'upperBound': 20
        },
        'AGMT_COMM_TXN': {
            'sql_query': '(SELECT DISTINCT COMM_TXN_ID,CYCLE_DT,AGMT_ID,MNO_PAYABLE_AMT FROM PROD_STND_TBLS.AGMT_COMM_TXN) as subquery',
            'numPartitions': 300,
            'partitionColummn': '(COMM_TXN_ID MOD 300)',
            'lowerBound': 0,
            'upperBound': 300
        },
        'COMM_FACT': {
            'sql_query': '(SELECT DISTINCT COMM_TXN_ID,LST_MOD_TS,PROD_SID,COMM_AMT_FYC,CYCLE_DT,SEC_DT,AGMT_SID,ADVSR_SID,UNIT_NR FROM PROD_USIG_CRCOG_DM.COMM_FACT) as subquery',
            'numPartitions': 100,
            'partitionColummn': '(COMM_TXN_ID MOD 100)',
            'lowerBound': 0,
            'upperBound': 100
        },
        'PREMIUM_FACT': {
            'sql_query': '(SELECT DISTINCT PREM_TXN_ID,LST_MOD_TS,PROD_SID,AGMT_SID,PREM_AMT_FY,CYCLE_DT,SEC_DT,POL_CR_AMT,ADVSR_SID,UNIT_NR FROM PROD_USIG_CRCOG_DM.PREMIUM_FACT) as subquery',
            'numPartitions': 30,
            'partitionColummn': '(PREM_TXN_ID MOD 30)',
            'lowerBound': 0,
            'upperBound': 30
        },
        'PROD': {
            'sql_query': '(SELECT DISTINCT PROD_SID,LST_MOD_TS,LOB_CDE,PROD_TYP_CDE,QUAL_CDE,SER_YR,PROD_TIER_NR,XCS_BSIC_CDE,ADMN_SYS_CDE,IMPLT_DT,SLS_RPTG_MAJOR_GRP_CDE,SLS_RPTG_MINOR_GRP_CDE,SLS_RPTG_PROD_TYP_CDE FROM PROD_USIG_CRCOG_DM.PROD) as subquery',
            'numPartitions': 2,
            'partitionColummn': '(PROD_SID MOD 2)',
            'lowerBound': 0,
            'upperBound': 2
        }
    }
}
"
