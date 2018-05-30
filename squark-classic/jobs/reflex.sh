export PROJECT_ID=reflex
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export EXCLUDE_TABLES="CMB_PARTC_TOT_MILESTONE_WORK,DB_RPT_REQUEST_TRACKING_WORK"
export INCLUDE_TABLES="PERSON,EXTERNAL_ADDRESS,partc_status_cde,PLAN_STATUS_CDE,PRODUCT_TYPE_CDE,MARITAL_STATUS_CDE,PARTC_STATUS_HIST,PARTC_SOURCE,PLAN_DCO,PARTICIPANT,PLAN_SPONSOR,PLAN_SUBSCRIPTION,BUSINESS_ENTITY,PARTC_SRC_ADI_EVENT,PARTC_SRC_ADI_EVENT_HIST,CENSUS_EE,BENEFICIARY,PARTICIPANT_LOG,PERSON_LOG,BENEFICIARY_LOG,PLAN_SOURCE,MEMBER_STATUS_HISTORY,ISW_SUSPENSION,MEMBERS,DATA_EXCHANGE_HEADER,DATA_EXCHANGE_DEMO,PAYROLL_HISTORY,XPNS_INTERMITTENT_PIT,SPON_ACTIV_GRP,SPON_ACTIV_GRP_OPTION,PARTN1_PARTC_PERIODIC_BAL"
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export CONNECTION_ID=reflex


export JSON_INFO='
{
    "PARTITION_INFO":{
        "tables": {
            "DATA_EXCHANGE_DEMO": {
              "partitionColumn": "DATA_EXCH_ID",
              "lowerBound": 2212800,
              "upperBound": 9000000,
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
              "lowerBound": 3,
              "upperBound": 216000,
              "numPartitions": 50
            },
            "CENSUS_EE": {
              "partitionColumn": "SUBSCRIBER_ID",
              "lowerBound": 100,
              "upperBound": 216000,
              "numPartitions": 50
            },
            "PERSON_LOG": {
              "partitionColumn": "DATEPART(SECOND, UPDATE_DATETIME)",
              "lowerBound": 0,
              "upperBound": 59,
              "numPartitions": 60
            }
        }
   }
}
'




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
