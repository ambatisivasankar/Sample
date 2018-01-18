export PROJECT_ID=lms_analytics
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=lms_analytics
# export EXCLUDE_TABLES="ACXIOM_PROSPECT,LEADANALYTICS_ACXIOM,LEADANALYTICS_ACXIOM_PRIMARY_DATA"
export INCLUDE_TABLES="ANALYTICS,LEAD_DEMOGRAPHICS,SCORE_VERSION_LKP,HomeOwner_Mortgage,LEADANALYTICS_ACXIOM_TRNSLT,LEADANALYTICS_CAMPAIGN,LEADANALYTICS_SOURCECODE_MASTER,LEADANALYTICS_CAMPAIGN_CDE_TRNSLT,LEADANALYTICS_PERSON,LEADANALYTICS_PROSPECT,LEADANALYTICS_SALES_PRODUCT_TRANSLATION,SALESFORCE_LEAD_STAGING,LEADANALYTICS_LMS_MATCH,LEADANALYTICS_PROSPECTS_LEADS,PS_ZIP_AGENCY_FIRM,PS_AGENCY_TRANSLATION,PS_AGENT_CREDENTIALS,PS_AGENT_CREDENTIALS_UPDATES,PS_TRUSTED_ADVISOR,PS_ELF_DATA,PS_ADD_LEAD_DETAILS"
export CONNECTION_ID=lms_analytics
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
