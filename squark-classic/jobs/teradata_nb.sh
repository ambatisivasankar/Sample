export PROJECT_ID=teradata_nb
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='NB_APPL_VW,NB_COV_RISK_VW,NB_PRTY_APPL_RLE_VW,NB_APPL_PRTY_VW,NB_PRTY_CASE_OWN_VW,DI_NB_RPT_VW,NB_PRTY_APPL_AD_VW,INSURED_VW,NB_BILL_INFO_VW,NB_RPT_VW,NB_RIDER_INFO_VW'
export CONNECTION_ID=teradata_nb

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'NB_APPL_VW': {
              'partitionColumn': 'APPL_ID',
              'lowerBound': 1,
              'upperBound': 40000000,
              'numPartitions': 50
            },
            'NB_PRTY_APPL_RLE_VW': {
              'partitionColumn': 'PRTY_APL_AD_ID',
              'lowerBound': 0,
              'upperBound': 4612000,
              'numPartitions': 50
            }
        }
   }
}
"




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
