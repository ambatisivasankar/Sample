export PROJECT_ID=trac_rpgw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export SKIP_MIN_MAX_ON_CAST=1
export CONNECTION_ID=trac_rpgw
# connection attempt on aws jenkins-ingestion fails without below TZ setting: ORA-01882: timezone region  not found
# 2018.06.24 TZ still needs to be set
export TZ=GMT
export INCLUDE_TABLES='DIM_ACCOUNT_ADMNSTRTR,DIM_AFFILIATE_COMPANY,DIM_BENEFICIARY,DIM_CONSENT_CUST,DIM_EMPLOYEE,DIM_ICU,DIM_PARTICIPANT,DIM_PARTICIPANT_ACCOUNT,DIM_PLAN,DIM_PLAN_ACCOUNT,DIM_RETIREMENT_PROJECTION,DIM_SPONSOR_COMPANY,DIM_VRU_ENROLLMENT_STATUS,FACT_PRTCPNT_ASST_DTL'
export SPARK_MAX_EXECUTORS=20
export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "DIM_CONSENT_CUST": {
              "partitionColumn": "CUSTOMER_ID2",
              "lowerBound": 0,
              "upperBound": 94294966676,
              "numPartitions": 50
            },
            "DIM_BENEFICIARY": {
              "partitionColumn": "BNFCRY_KEY",
              "lowerBound": 1000000,
              "upperBound": 3000000,
              "numPartitions": 100
            },
            "DIM_VRU_ENROLLMENT_STATUS": {
              "partitionColumn": "INTRNL_PLAN_ID",
              "lowerBound": 0,
              "upperBound": 50000,
              "numPartitions": 50
            },
            "DIM_PARTICIPANT_ALLCTN_STP": {
              "partitionColumn": "rcrd_trm_dt >= """01-JAN-2016""" AND PRTCPNT_ALLCTN_STP_KEY",
              "lowerBound": 0,
              "upperBound": 550000000,
              "numPartitions": 50
            },
            "DIM_EMPLOYEE": {
              "partitionColumn": "ACTV_RCRD_IND = """Y""" AND EMPLY_KEY",
              "lowerBound": 29000000,
              "upperBound": 90000000,
              "numPartitions": 50
            },
            "DIM_PLAN": {
              "partitionColumn": "ACTV_RCRD_IND = """Y""" AND PLAN_KEY",
              "lowerBound": 14000000,
              "upperBound": 15000000,
              "numPartitions": 50
            },
            "FACT_PLAN_ACCOUNT_FEE": {
              "partitionColumn": "PLAN_ACCNT_KEY",
              "lowerBound": 700000,
              "upperBound": 2100000,
              "numPartitions": 50
            },
            "FACT_PRTCPNT_ASST_DTL": {
              "partitionColumn": "ASST_AS_OF_DT >= """01-JAN-2016""" AND PLAN_FUND_SUB_ACCNT_KEY",
              "lowerBound": 2500000,
              "upperBound": 6500000,
              "numPartitions": 100
            },
            "DIM_PARTICIPANT": {
              "partitionColumn": "RCRD_TRM_DT >= """01-JAN-2016""" AND PRTCPNT_KEY",
              "lowerBound": 168220000,
              "upperBound": 240000000,
              "numPartitions": 100
            },
            "DIM_PARTICIPANT_ACCOUNT": {
              "partitionColumn": "PRTCPNT_ACCNT_KEY",
              "lowerBound": 50000000,
              "upperBound": 100000000,
              "numPartitions": 100
            }
        }
   }
}
'

#export JSON_INFO="
#{'SAVE_TABLE_SQL_SUBQUERY':{
#      'schema': 'RPW_DIM_FACT',
#      'table_queries': {
#        'DIM_PLAN': '(SELECT * FROM RPW_DIM_FACT.DIM_PLAN WHERE ACTV_RCRD_IND = '''Y''')',
#        'DIM_PARTICIPANT': '(SELECT * FROM RPW_DIM_FACT.DIM_PARTICIPANT WHERE ACTV_RCRD_IND = '''Y''')'
#      }
# }}
#"
