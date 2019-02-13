export PROJECT_ID=reflex
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export CONNECTION_ID=reflex

# NOTE: It's critical that these array variables have a different
# name than the actual variables that will be used later by Squark.
# See: https://stackoverflow.com/q/51272394/877069
EXCLUDE_TABLES_ARRAY=(
  "CMB_PARTC_TOT_MILESTONE_WORK"
  "DB_RPT_REQUEST_TRACKING_WORK"
)
INCLUDE_TABLES_ARRAY=(
  "PERSON"
  "EXTERNAL_ADDRESS"
  "partc_status_cde"
  "PLAN_STATUS_CDE"
  "PRODUCT_TYPE_CDE"
  "MARITAL_STATUS_CDE"
  "PARTC_STATUS_HIST"
  "PARTC_SOURCE"
  "PLAN_DCO"
  "PARTICIPANT"
  "PLAN_SPONSOR"
  "PLAN_SUBSCRIPTION"
  "BUSINESS_ENTITY"
  "PARTC_SRC_ADI_EVENT"
  "PARTC_SRC_ADI_EVENT_HIST"
  "CENSUS_EE"
  "BENEFICIARY"
  "PARTICIPANT_LOG"
  "PERSON_LOG"
  "BENEFICIARY_LOG"
  "PLAN_SOURCE"
  "MEMBER_STATUS_HISTORY"
  "ISW_SUSPENSION"
  "MEMBERS"
  "DATA_EXCHANGE_HEADER"
  "DATA_EXCHANGE_DEMO"
  "PAYROLL_HISTORY"
  "XPNS_INTERMITTENT_PIT"
  "SPON_ACTIV_GRP"
  "SPON_ACTIV_GRP_OPTION"
  "PARTN1_PARTC_PERIODIC_BAL"
  "RMAP_EXTRACT_REQ"
  "MM_EMPLOYEE"
  "SPONSOR_PSR"
  "FUND_ACTIVITY"
  "PLAN_FUND_CMPNT"
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
              "lowerBound": 1000,
              "upperBound": 216000,
              "numPartitions": 50
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
