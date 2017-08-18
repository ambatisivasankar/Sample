export PROJECT_ID=trac_rpgw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
#export INCLUDE_TABLES="FACT_PLAN_SOURCE_SUMMARY,FACT_DAILY_INCOME,FACT_LOAN_SOURCE_SUMMARY,FACT_PLAN_SUMMARY,FACT_PARTICIPANT_FEE,FACT_PRTCPNT_BLNC_SMMRY,FACT_PLAN_FNNCL_TRNSCTN,DIM_PARTICIPANT,FACT_PLAN_ACCOUNT_ASSETS,FACT_PLAN_ACCOUNT_FEE,DIM_PLAN,FACT_PRTCPNT_LOAN_SMMRY,FACT_PRTCPNT_ASST_DTL,FACT_SHORT_TERM_TRADE,FACT_PRTCPNT_LOAN_TRNSCTN,FACT_PRTCPNT_ASST_SMMRY,FACT_PARTICIPANT_ACCOUNT"
export INCLUDE_TABLES="DIM_PARTICIPANT"
export SKIP_MIN_MAX_ON_CAST=1
export CONNECTION_ID=trac_rpgw

# connection attempt on aws jenkins-ingestion fails without below TZ setting: ORA-01882: timezone region  not found
export TZ=GMT
export SPARK_MAX_EXECUTORS=10
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'DIM_PARTICIPANT': {
              'partitionColumn': 'PRTCPNT_KEY',
              'lowerBound': 1,
              'upperBound': 200000000,
              'numPartitions': 200
            }
        }
   }
}
"

#export JSON_INFO="
#{'SAVE_TABLE_SQL_SUBQUERY':{
#      'schema': 'RPW_DIM_FACT',
#      'table_queries': {
#        'DIM_PLAN': '(SELECT * FROM RPW_DIM_FACT.DIM_PLAN WHERE ACTV_RCRD_IND = '''Y''')',
#        'DIM_PARTICIPANT': '(SELECT * FROM RPW_DIM_FACT.DIM_PARTICIPANT WHERE ACTV_RCRD_IND = '''Y''')'
#      }
# }}
#"
