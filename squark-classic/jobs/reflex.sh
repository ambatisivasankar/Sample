export PROJECT_ID=reflex
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export EXCLUDE_TABLES="CMB_PARTC_TOT_MILESTONE_WORK,DB_RPT_REQUEST_TRACKING_WORK"
export INCLUDE_TABLES="PERSON,EXTERNAL_ADDRESS,partc_status_cde,PLAN_STATUS_CDE,PRODUCT_TYPE_CDE,MARITAL_STATUS_CDE,PARTC_STATUS_HIST,PARTC_STATUS_HIST,PARTC_SOURCE,PLAN_DCO,PARTICIPANT,PLAN_SPONSOR,PLAN_SUBSCRIPTION,BUSINESS_ENTITY,PARTC_SRC_ADI_EVENT,PARTC_SRC_ADI_EVENT_HIST,CENSUS_EE,BENEFICIARY,PARTICIPANT_LOG,PERSON_LOG,BENEFICIARY_LOG,PLAN_SOURCE,MEMBER_STATUS_HISTORY,ISW_SUSPENSION"
export INCLUDE_VIEWS=1
export SKIP_SOURCE_ROW_COUNT=1
export CONNECTION_ID=reflex
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
