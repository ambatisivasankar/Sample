export PROJECT_ID=teradata
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_CMN_VW,CUST_AGMT_CMN_VW,CUST_DEMOGRAPHICS_VW,PDCR_AGMT_CMN_VW,PDCR_DEMOGRAPHICS_VW,AGMT_UWRT_CMN_VW,PDCR_ALT_ID_CMN_VW,CUST_PREFERENCE_VW,SLLNG_AGMT_CMN_VW,AGMT_VAL_CMN_VW,AGMT_CVG_CMN_VW'
export CONNECTION_ID=teradata
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
