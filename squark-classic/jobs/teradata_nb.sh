export PROJECT_ID=teradata_nb
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='NB_APPL_VW,NB_COV_RISK_VW,NB_PRTY_APPL_RLE_VW,NB_APPL_PRTY_VW,NB_PRTY_CASE_OWN_VW,DI_NB_RPT_VW,NB_PRTY_APPL_AD_VW,NB_PENDING_INVNTRY_DTL_SRC_VW,INSURED_VW,NB_BILL_INFO_VW'
export CONNECTION_ID=teradata_nb
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
