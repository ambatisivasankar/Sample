# Required
export PROJECT_ID=reflex
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=reflex

# Optional
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1

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
              "partitionColumn": "DATA_EXCH_ID",
              "lowerBound": 2138100,
              "upperBound": 9630000,
              "numPartitions": 50
            },
            "PAYROLL_HISTORY": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 500,
              "upperBound": 215000,
              "numPartitions": 100
            },
            "PARTN1_PARTC_PERIODIC_BAL": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 0,
              "upperBound": 210000,
              "numPartitions": 50
            },
            "CENSUS_EE": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 100,
              "upperBound": 216000,
              "numPartitions": 50
            },
            "FUND_ACTIVITY": {
              "partitionColumn": "DATEPART(DAY, VALUATION_DT)",
              "lowerBound": 1,
              "upperBound": 31,
              "numPartitions": 40
            }
        }
   }
}
'

## 2018.06.04: this one not working out too well
#            "PERSON_LOG": {
#              "partitionColumn": "DATEPART(SECOND, UPDATE_DATETIME)",
#              "lowerBound": 0,
#              "upperBound": 59,
#              "numPartitions": 60
#            }


#export STATS_CONFIG="
#{
#    'profiles': {
#        'numeric': ['max','min','mean','countDistinct','count_null'],
#        'string': ['max', 'min','countDistinct','count_null'],
#        'datetype': ['max', 'min','countDistinct','count_null']
#    },
#    'field_types': {
#        'NumericType': 'numeric',
#        'StringType': 'string',
#        'DateType': 'datetype',
#        'TimestampType': 'datetype'
#    }
#}
#"
