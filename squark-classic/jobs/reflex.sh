# Required
export PROJECT_ID=reflex
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=reflex

# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export SPARK_EXECUTOR_MEMORY="8G"

EXCLUDE_TABLES_ARRAY=(
  "CMB_PARTC_TOT_MILESTONE_WORK"
  "DB_RPT_REQUEST_TRACKING_WORK"
)
INCLUDE_TABLES_ARRAY=(
  "BENEFICIARY"
  "BENEFICIARY_LOG"
  "BUSINESS_ENTITY"
  "CENSUS_EE"
  "COUNTRY_CDE"
  "DATA_EXCHANGE_DEMO"
  "DATA_EXCHANGE_HEADER"
  "EXTERNAL_ADDRESS"
  "FUND"
  "FUND_ACTIVITY"
  "FUND_MSTR"
  "ISW_SUSPENSION"
  "MARITAL_STATUS_CDE"
  "MEMBER_STATUS_HISTORY"
  "MEMBERS"
  "MEMBER_STATUS_CDE"
  "MM_EMPLOYEE"
  "PARTC_SOURCE"
  "PARTC_SRC_ADI_EVENT"
  "PARTC_SRC_ADI_EVENT_HIST"
  "PARTC_STATUS_CDE"
  "PARTC_STATUS_HIST"
  "PARTC_TYPE_CDE"
  "PARTICIPANT"
  "PARTICIPANT_LOG"
  "PARTN1_PARTC_PERIODIC_BAL"
  "PAYROLL_HISTORY"
  "PERSON"
  "PERSON_LOG"
  "PLAN_DCO"
  "PLAN_FUND_CMPNT"
  "PLAN_SOURCE"
  "PLAN_SPONSOR"
  "PLAN_STATUS_CDE"
  "PLAN_SUBSCRIPTION"
  "PRODUCT_TYPE_CDE"
  "PROVIDER"
  "RMAP_EXTRACT_REQ"
  "SOURCE"
  "SPON_ACTIV_GRP"
  "SPON_ACTIV_GRP_OPTION"
  "SPONSOR_PSR"
  "STATE_CDE"
  "XPNS_INTERMITTENT_PIT"
)

export EXCLUDE_TABLES="$(IFS=, ; echo "${EXCLUDE_TABLES_ARRAY[*]}")"
export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

export SPARK_MAX_EXECUTORS=10
# Do not use modulous (MOD, %) on this job.
export JSON_INFO='
{
    "SUPER_PROJECTION_SETTINGS":{
        "tables": {
            "COUNTRY_CDE": {
                "projection_name": "COUNTRY_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,COUNTRY_CDE,COUNTRY_DES",
                "segment_by_columns": ""
            },
            "MARITAL_STATUS_CDE": {
                "projection_name": "MARITAL_STATUS_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,MARITAL_STATUS_CDE,MARITAL_STATUS_DES",
                "segment_by_columns": ""
            },
            "MEMBER_STATUS_CDE": {
                "projection_name": "MEMBER_STATUS_CDE_SQUARK",
                "order_by_columns": "MEMBER_STATUS_CDE,MEMBER_STATUS_DES",
                "segment_by_columns": ""
            },
            "PARTC_STATUS_CDE": {
                "projection_name": "PARTC_STATUS_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,PARTC_STATUS_CDE,PARTC_STATUS_DES,ACTIVITY_ID",
                "segment_by_columns": ""
            },
            "PARTC_STATUS_HIST": {
                "projection_name": "PARTC_STATUS_HIST_SQUARK",
                "order_by_columns": "PARTC_STATUS_CDE,SPONSOR_ID,PLAN_SEQNR,SUBSCRIBER_ID,PARTICIPANT_ID",
                "segment_by_columns": "PARTICIPANT_ID"
            },
            "PARTC_TYPE_CDE": {
                "projection_name": "PARTC_TYPE_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,PARTC_TYPE_CDE,PARTC_TYPE_DES",
                "segment_by_columns": ""
            },
            "PARTICIPANT": {
                "projection_name": "PARTICIPANT_SQUARK",
                "order_by_columns": "SPONSOR_ID,PLAN_SEQNR,SUBSCRIBER_ID,PARTICIPANT_ID",
                "segment_by_columns": "PARTICIPANT_ID"
            },
            "PERSON": {
                "projection_name": "PERSON_SQUARK",
                "order_by_columns": "PERSON_ID",
                "segment_by_columns": "PERSON_ID"
            },
            "PLAN_DCO": {
                "projection_name": "PLAN_DCO_SQUARK",
                "order_by_columns": "SPONSOR_ID,PLAN_SEQNR",
                "segment_by_columns": ""
            },
            "PLAN_STATUS_CDE": {
                "projection_name": "PLAN_STATUS_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,PLAN_STATUS_CDE,PLAN_STATUS_DES",
                "segment_by_columns": ""
            },
            "PRODUCT_TYPE_CDE": {
                "projection_name": "PRODUCT_TYPE_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,PRODUCT_TYPE_CDE,PRODUCT_TYPE_DES",
                "segment_by_columns": ""
            },
            "STATE_CDE": {
                "projection_name": "STATE_CDE_SQUARK",
                "order_by_columns": "UPDATE_NR,COUNTRY_CDE,STATE_CDE,STATE_DES,CAS_WITHHOLDING_GL_NR",
                "segment_by_columns": ""
            }
        }
    },
    "PARTITION_INFO":{
        "tables": {
            "DATA_EXCHANGE_DEMO": {
              "partitionColumn": "DATA_EXCH_ID",
              "lowerBound": 2100000,
              "upperBound": 11000000,
              "numPartitions": 200
            },
            "CENSUS_EE": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 0,
              "upperBound": 200000,
              "numPartitions": 50
            },
            "EXTERNAL_ADDRESS": {
              "partitionColumn": "ADDRESS_ID",
              "lowerBound": 0,
              "upperBound": 15000000,
              "numPartitions": 50
            },
            "FUND_ACTIVITY": {
              "partitionColumn": "DATEPART(YEAR, VALUATION_DT)",
              "lowerBound": 2000,
              "upperBound": 2020,
              "numPartitions": 20
            },
            "MEMBER_STATUS_HISTORY": {
              "partitionColumn": "DATEPART(YEAR, EFF_FROM_DT)",
              "lowerBound": 2000,
              "upperBound": 2020,
              "numPartitions": 20
            },
            "PARTC_SOURCE": {
              "partitionColumn": "DATEPART(DAY, UPDATE_DATETIME)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTICIPANT": {
              "partitionColumn": "DATEPART(DAY, UPDATE_DATETIME)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTICIPANT_LOG": {
              "partitionColumn": "DATEPART(DAY, UPDATE_DATETIME)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTN1_PARTC_PERIODIC_BAL": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 0,
              "upperBound": 230000,
              "numPartitions": 50
            },
            "PAYROLL_HISTORY": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 0,
              "upperBound": 250000,
              "numPartitions": 100
            },
            "PERSON": {
              "partitionColumn": "DATEPART(DAY, UPDATE_DATETIME)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PERSON_LOG": {
              "partitionColumn": "DATEPART(DAY, UPDATE_DATETIME)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            }
        }
   }
}
'
