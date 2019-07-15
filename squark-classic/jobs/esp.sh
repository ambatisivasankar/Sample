# Required
export PROJECT_ID=esp
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'

# Optional
export INCLUDE_VIEWS=1
export CONNECTION_ID=esp
export SPARK_MAX_EXECUTORS=15

EXCLUDE_TABLES_ARRAY=(
  "ANLTCS_NEXT_PURCHASE"
  "IDM_ACCOUNT_20171206"
  "IDM_POLICYDETAIL_20161021"
  "MET_NONREG_PAYMENT_BKP112017"
  "SF_MM_ACCOUNT_20160927"
  "SF_MM_ACCOUNT_20170118"
  "SF_MM_ACCOUNT_bkp_0129"
  "SF_MM_COMM_LIST_bkp_1029"
  "SF_MM_CUST_LIST_BKP20180131"
  "SF_MM_HOLDING_0914"
  "SF_MM_HOLDING_bkp_1205"
)
export EXCLUDE_TABLES="$(IFS=, ; echo "${EXCLUDE_TABLES_ARRAY[*]}")"

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'ANLTCS_LEADS': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM SCORE_DT), 0)',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'ANLTCS_LEADS_0813181800': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM SCORE_DT), 0)',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'ANLTCS_LEADS_20190116_1141': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM SCORE_DT), 0)',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'CATS_WRK': {
              'partitionColumn': 'WRK_IDENT MOD 100',
              'lowerBound': 0,
              'upperBound': 100,
              'numPartitions': 100
            },
            'CATS_WRK_TXT_CMNT': {
              'partitionColumn': 'FK_WRK_IDENT MOD 100',
              'lowerBound': 0,
              'upperBound': 100,
              'numPartitions': 100
            },
             'CLIENT_LIST_PHV': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM BRTH_DT), 0)',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'CSP_WORK_INTERACTION': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM PXCREATEDATETIME), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
             'CSP_WORK_SERVICE': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM PYAGEFROMDATE), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'CSTM_PDCR_RPT': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM CAL_DT), 0)',
              'lowerBound': 1,
              'upperBound': 30,
              'numPartitions': 30
            },
            'IDM_ACCOUNT_PARTY': {
              'partitionColumn': 'AccountId MOD 50',
              'lowerBound': 0,
              'upperBound': 50,
              'numPartitions': 50
            },
            'IDM_ACCOUNT_PARTY_ADDRESS': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastUpdateDT), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'MET_NONREG_PAYMENT': {
              'partitionColumn': 'COALESCE(EXTRACT(DAY FROM PAID_DATE), 0)',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'SF_MM_ACCOUNT': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_ACCOUNT_pre1Updates': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_AGMT_ADDRESS': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_AGMT_CASH_VALUE': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_AGMT_REL_LIST': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_CAMPAIGN_MEMBER': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM CreatedDate), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_COMM_LIST': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_HLDG_SVC_TRCKNG_LST': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM SF_LST_UPD_DT_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_HOLDING': {
              'partitionColumn': 'COALESCE(EXTRACT(MINUTE FROM CreatedDate), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_HOLDING_2': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM LastModifiedDate_UTC), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'SF_MM_LEADS_x': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM CreatedDate), 0) MOD 15',
              'lowerBound': 0,
              'upperBound': 15,
              'numPartitions': 15
            },
            'SF_MM_LEADS_20190116_1141': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM CreatedDate), 0) MOD 15',
              'lowerBound': 0,
              'upperBound': 15,
              'numPartitions': 15
            },
            'SF_MM_LEADS': {
              'partitionColumn': 'COALESCE(EXTRACT(SECOND FROM CreatedDate), 0)',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            }
		}
   }
}
"
