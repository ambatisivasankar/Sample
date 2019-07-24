# Required
export PROJECT_ID=reflex
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=reflex

# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export SPARK_EXECUTOR_MEMORY="2G"

EXCLUDE_TABLES_ARRAY=(
  "CMB_PARTC_TOT_MILESTONE_WORK"
  "DB_RPT_REQUEST_TRACKING_WORK"
)
INCLUDE_TABLES_ARRAY=(
  "BENEFICIARY"
  "BENEFICIARY_LOG"
  "BUSINESS_ENTITY"
  "CENSUS_EE"
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
  "MM_EMPLOYEE"
  "PARTC_SOURCE"
  "PARTC_SRC_ADI_EVENT"
  "PARTC_SRC_ADI_EVENT_HIST"
  "partc_status_cde"
  "PARTC_STATUS_HIST"
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
  "XPNS_INTERMITTENT_PIT"
)

export EXCLUDE_TABLES="$(IFS=, ; echo "${EXCLUDE_TABLES_ARRAY[*]}")"
export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

export SPARK_MAX_EXECUTORS=10
export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "DATA_EXCHANGE_DEMO": {
              "partitionColumn": "SSN % 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "CENSUS_EE": {
              "partitionColumn": "COALESCE(DATEPART(MINUTE, EFF_FROM_DT), 0) % 20",
              "lowerBound": 0,
              "upperBound": 20,
              "numPartitions": 20
            },
            "EXTERNAL_ADDRESS": {
              "partitionColumn": "COALESCE(DATEPART(MINUTE, UPDATE_DATETIME), 1)",
              "lowerBound": 0,
              "upperBound": 59,
              "numPartitions": 60
            },
            "FUND_ACTIVITY": {
              "partitionColumn": "COALESCE(DATEPART(DAY, VALUATION_DT), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "MEMBER_STATUS_HISTORY": {
              "partitionColumn": "COALESCE(DATEPART(DAY, UPDATE_DATETIME), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTC_SOURCE": {
              "partitionColumn": "COALESCE(DATEPART(DAY, UPDATE_DATETIME), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTICIPANT": {
              "partitionColumn": "COALESCE(DATEPART(DAY, UPDATE_DATETIME), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTICIPANT_LOG": {
              "partitionColumn": "COALESCE(DATEPART(DAY, UPDATE_DATETIME), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PARTN1_PARTC_PERIODIC_BAL": {
              "partitionColumn": "SHARES % 50",
              "lowerBound": 0,
              "upperBound": 50,
              "numPartitions": 50
            },
            "PAYROLL_HISTORY": {
              "partitionColumn": COALESCE(DATEPART(SECOND, UPDATE_DATETIME), 0)",
              "lowerBound": 0,
              "upperBound": 59,
              "numPartitions": 60
            },
            "PERSON": {
              "partitionColumn": "COALESCE(DATEPART(DAY, UPDATE_DATETIME), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            },
            "PERSON_LOG": {
              "partitionColumn": "COALESCE(DATEPART(DAY, UPDATE_DATETIME), 1)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 31
            }
        }
   }
}
'
