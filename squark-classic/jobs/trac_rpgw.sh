# Required
export PROJECT_ID=trac_rpgw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=trac_rpgw

# Optional
export SKIP_MIN_MAX_ON_CAST=1
export TZ=GMT
export SPARK_MAX_EXECUTORS=20

INCLUDE_TABLES_ARRAY=(
  "DIM_ACCOUNT_ADMNSTRTR"
  "DIM_AFFILIATE_COMPANY"
  "DIM_BENEFICIARY"
  "DIM_CONSENT_CUST"
  "DIM_EMPLOYEE"
  "DIM_ICU"
  "DIM_INVESTMENT_VEHICLE"
  "DIM_PARTICIPANT"
  "DIM_PARTICIPANT_ACCOUNT"
  "DIM_PLAN"
  "DIM_PLAN_ACCOUNT"
  "DIM_RETIREMENT_PROJECTION"
  "DIM_SPONSOR_COMPANY"
  "DIM_VRU_ENROLLMENT_STATUS"
  "FACT_PRTCPNT_ASST_DTL"
)
export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

export JSON_INFO='
{ "SUPER_PROJECTION_SETTINGS":
    {
        "tables":
        {
            "DIM_PARTICIPANT":
            {
                "projection_name": "DIM_PARTICIPANT_SQUARK",
                "order_by_columns": "ACTV_RCRD_IND,ICU_ID,AFFLT_CMPNY_ID,INTRNL_PLAN_ID,SPNSR_CMPNY_ID,PLAN_ID,EMPLY_TAX_ID,PRTCPNT_ID,PRTCPNT_KEY",
                "segment_by_columns": "PRTCPNT_ID"
            },
            "DIM_VRU_ENROLLMENT_STATUS":
            {
                "projection_name": "DIM_VRU_ENROLLMENT_STATUS_SQUARK",
                "order_by_columns": "ACTV_RCRD_IND,ICU_ID,ENRLLMNT_STTS_CD,ENRLLMNT_STTS_RPRT_DT,PLAN_ENRLLMNT_DT,INTRNL_PLAN_ID,PLAN_ID,PRTCPNT_ID,VRU_ENRLLMNT_STTS_KEY",
                "segment_by_columns": "PRTCPNT_ID"
            },
            "DIM_PLAN":
            {
                "projection_name": "DIM_PLAN_SQUARK",
                "order_by_columns": "ACTV_RCRD_IND,ICU_ID,PLAN_STTS_DSCRPTN,PLAN_TYPE_CD,PLAN_ID,PLAN_KEY",
                "segment_by_columns": ""
            },
            "DIM_EMPLOYEE":
            {
                "projection_name": "DIM_EMPLOYEE_SQUARK",
                "order_by_columns": "ACTV_RCRD_IND,ICU_ID,AFFLT_CMPNY_ID,SPNSR_CMPNY_ID,EMPLY_TAX_ID,EMPLY_KEY",
                "segment_by_columns": "EMPLY_TAX_ID"
            },
            "DIM_SPONSOR_COMPANY":
            {
                "projection_name": "DIM_SPONSOR_COMPANY_SQUARK",
                "order_by_columns": "ICU_ID,SPNSR_CMPNY_TYPE_CD,SPNSR_CMPNY_ID,SPNSR_CMPNY_TAX_ID,SPNSR_CMPNY_NM,SPNSR_CMPNY_KEY",
                "segment_by_columns": ""
            }

        }
    },
    "PARTITION_INFO":{
        "tables": {
            "DIM_BENEFICIARY": {
              "partitionColumn": "MOD(BNFCRY_KEY, 20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "DIM_CONSENT_CUST": {
              "partitionColumn": "MOD(CONSENT_CUST_KEY, 20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "DIM_EMPLOYEE": {
              "partitionColumn": "ACTV_RCRD_IND = """Y""" AND MOD(EMPLY_KEY,20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "DIM_INVESTMENT_VEHICLE": {
              "partitionColumn": "MOD(INVSTMNT_VHCL_KEY, 20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "DIM_PARTICIPANT": {
              "partitionColumn": "RCRD_TRM_DT >= """01-JAN-2016""" AND PRTCPNT_KEY",
              "lowerBound": 160000000,
              "upperBound": 240000000,
              "numPartitions": 100
            },
            "DIM_PARTICIPANT_ACCOUNT": {
              "partitionColumn": "MOD(PRTCPNT_ACCNT_KEY, 20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "DIM_PLAN": {
              "partitionColumn": "ACTV_RCRD_IND = """Y""" AND MOD(PLAN_KEY, 20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "DIM_VRU_ENROLLMENT_STATUS": {
              "partitionColumn": "MOD(VRU_ENRLLMNT_STTS_KEY, 20)",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "FACT_PRTCPNT_ASST_DTL": {
              "partitionColumn": "ASST_AS_OF_DT >= """01-JAN-2016""" AND PLAN_FUND_SUB_ACCNT_KEY",
              "lowerBound": 2500000,
              "upperBound": 6000000,
              "numPartitions": 100
            }
        }
   }
}
'
