
export PROJECT_ID=teradata_siera
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT,AGMT_COMM_TXN,COMM_FACT,PREMIUM_FACT,PROD'
export CONNECTION_ID=drc_sierra
export SKIP_SOURCE_ROW_COUNT=1
export SPARK_MAX_EXECUTORS=6
# export SPARK_EXECUTOR_MEMORY="4G"
echo "Start dt: " $strt_dt
echo "End Dt: " $end_dt

#   in order to use value in the subfilter, double-quote all innards of the JSON

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'PREMIUM_FACT': {
            'sql_query': '(SELECT PF.PREM_TXN_ID,PF.LST_MOD_TS,PF.PROD_SID,PF.AGMT_SID,PF.PREM_AMT_FY,PF.CYCLE_DT,PF.SEC_DT,PF.POL_CR_AMT FROM PROD_USIG_CRCOG_DM.PREMIUM_FACT PF WHERE CAST(PF.LST_MOD_TS AS DATE) BETWEEN CAST('''$strt_dt''' AS DATE)  AND CAST('''$end_dt''' AS DATE)) as subquery',
            'numPartitions': 30,
            'partitionColummn': '(PREM_TXN_ID MOD 30)',
            'lowerBound': 0,
            'upperBound': 29
        },
        'PROD': {
            'sql_query': '(SELECT P.PROD_SID,P.LST_MOD_TS,P.LOB_CDE FROM PROD_USIG_CRCOG_DM.PROD P WHERE CAST(P.LST_MOD_TS AS DATE) BETWEEN CAST('''$strt_dt''' AS DATE) AND CAST('''$end_dt''' AS DATE)) as subquery',
            'numPartitions': 2,
            'partitionColummn': '(PROD_SID MOD 2)',
            'lowerBound': 0,
            'upperBound': 1
        },
        'AGMT': {
            'sql_query': '(SELECT A.AGMT_SID,A.LST_MOD_TS,A.HLDG_KEY_PFX,A.HLDG_KEY,A.HLDG_KEY_SFX FROM PROD_USIG_CRCOG_DM.AGMT A WHERE CAST(A.LST_MOD_TS AS DATE) between cast('''$strt_dt''' as date)  AND cast('''$end_dt''' as date)) as subquery',
            'numPartitions': 20,
            'partitionColummn': '(AGMT_SID MOD 20)',
            'lowerBound': 0,
            'upperBound': 19
        },
        'COMM_FACT': {
            'sql_query': '(SELECT CF.COMM_TXN_ID,CF.LST_MOD_TS,CF.PROD_SID,CF.COMM_AMT_FYC,CF.CYCLE_DT,CF.SEC_DT FROM PROD_USIG_CRCOG_DM.COMM_FACT CF WHERE CAST(CF.LST_MOD_TS AS DATE) between cast('''$strt_dt''' as date)  AND cast('''$end_dt''' as date)) as subquery',
            'numPartitions': 100,
            'partitionColummn': '(COMM_TXN_ID MOD 100)',
            'lowerBound': 0,
            'upperBound': 99
        },
        'AGMT_COMM_TXN': {
            'sql_query': '(SELECT ACT.COMM_TXN_ID,ACT.CYCLE_DT,ACT.AGMT_ID,ACT.MNO_PAYABLE_AMT FROM PROD_STND_TBLS.AGMT_COMM_TXN ACT WHERE CAST(ACT.CYCLE_DT as DATE) between cast('''$strt_dt''' as date)  AND cast('''$end_dt''' as date)) as subquery',
            'numPartitions': 300,
            'partitionColummn': '(COMM_TXN_ID MOD 300)',
            'lowerBound': 0,
            'upperBound': 299
        }
    }
}
"
