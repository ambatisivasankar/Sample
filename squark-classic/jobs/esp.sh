export PROJECT_ID=esp
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=esp
export EXCLUDE_TABLES="ANLTCS_NEXT_PURCHASE,IDM_ACCOUNT_20171206,IDM_POLICYDETAIL_20161021,MET_NONREG_PAYMENT_BKP112017,SF_MM_ACCOUNT_20160927,SF_MM_ACCOUNT_20170118,SF_MM_ACCOUNT_bkp_0129,SF_MM_COMM_LIST_bkp_1029,SF_MM_CUST_LIST_BKP20180131,SF_MM_HOLDING_0914,SF_MM_HOLDING_bkp_1205,IDM_ACCOUNT_20171206,IDM_POLICYDETAIL_20161021,MET_NONREG_PAYMENT_BKP112017,SF_MM_ACCOUNT_20160927,SF_MM_ACCOUNT_20170118,SF_MM_ACCOUNT_bkp_0129,SF_MM_COMM_LIST_bkp_1029,SF_MM_CUST_LIST_BKP20180131,SF_MM_HOLDING_0914,SF_MM_HOLDING_bkp_1205"

export SPARK_MAX_EXECUTORS=15
# 2018.05.03, leave the orphaned partitioning from any newly excluded tables... you never know
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'CATS_WRK_TXT_CMNT': {
              'partitionColumn': 'FK_WRK_IDENT',
              'lowerBound': 5000000000,
              'upperBound': 5095000000,
              'numPartitions': 50
            },
            'CATS_WRK': {
              'partitionColumn': 'WRK_IDENT',
              'lowerBound': 5000000000,
              'upperBound': 5095000000,
              'numPartitions': 50
            },
            'CSTM_PDCR_RPT': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM CAL_DT), 0)',
              'lowerBound': 1,
              'upperBound': 30,
              'numPartitions': 30
            },
            'SF_MM_ACCOUNT': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_HOLDING': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM CreatedDate), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_ACCOUNT_pre1Updates': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_ACCOUNT_20170118': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_ACCOUNT_20160927': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'IDM_ACCOUNT_PARTY': {
              'partitionColumn': 'AccountId',
              'lowerBound': 1,
              'upperBound': 11700000,
              'numPartitions': 50
            },
            'IDM_ACCOUNT_PARTY_ADDRESS': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastUpdateDT), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_AGMT_ADDRESS': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_AGMT_CASH_VALUE': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_AGMT_REL_LIST': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_COMM_LIST': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_HLDG_SVC_TRCKNG_LST': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_HOLDING_2': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'SF_MM_LEADS': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM CreatedDate), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
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
