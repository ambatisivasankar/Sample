export PROJECT_ID=teradata_usig_stnd_vw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES="CUST_ADDL_TEL_NR_CMN_VW,CUST_ADDL_ELEC_AD_CMN_VW,PDCR_AGMT_CMN_VW,AGMT_CMN_VW,SLLNG_AGMT_CMN_VW,SALES_HIERARCHY_VW"
export CONNECTION_ID=teradata_usig_stnd_vw

export SPARK_MAX_EXECUTORS=10

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
            'PDCR_AGMT_CMN_VW': {
              'partitionColumn': 'AGREEMENT_ID',
              'lowerBound': 1,
              'upperBound': 34000000,
              'numPartitions': 50
            },
        }
   }
}
"