export PROJECT_ID=teradata_cmn
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='GENDER_CD_VW,RCOG_PROD_VW,SLS_DTRB_CAL_VW,DTH_CD_LKUP_VW,GOVT_ID_TYPE,AGMT_STUS_CD_LKUP,AGMT_STUS_RSN_CD_LKUP_VW,COMM_DT_VW,AGY_CLOSE_MERGE_DATA_VW,DSTRB_CHNL_CD_VW,MET_AGT_POINTINTIME_VW,MKT_INFO_CD_VW,MKT_TYP_CD_LKUP_VW,SRC_DATA_TRNSLT_VW,GOVT_ID_TYP_VW'
export CONNECTION_ID=teradata_cmn
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
