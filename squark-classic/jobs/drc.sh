export PROJECT_ID=drc
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=drc

export SPARK_EXECUTOR_MEMORY="4G"
export SPARK_MAX_EXECUTORS=15

export EXCLUDE_TABLES_ARRAY=(
    "BI_NFF_FACT_VW"
    "BI_SLS_DISB_FACT_VW"
    "TIER_NR"
)
export INCLUDE_TABLES_ARRAY=(
    "ADVSR_HIST_VW"
    "ADVSR_VW"
    "AGCY_AFFLTN_VW"
    "AGMT_VW"
    "CAL_VW"
    "CNTR_CLS_VW"
    "CNTR_TYP_VW"
    "CUST_VW"
    "DTCH_OFC_VW"
    "FLD_MGMT_HIER_VW"
    "GA_GROUP_VW"
    "GA_HIST_VW"
    "GOAL_AGGREGATE_VW"
    "GOAL_BRIDGE_VW"
    "GOAL_VW"
    "HIER_ROLE_VW"
    "NFF_FACT_VW"
    "NFF_HIST_VW"
    "PER_REP_FACT_VW"
    "PER_REP_VW"
    "PROD_GROUP_VW"
    "PROD_VW"
    "SLS_AND_DTRB_FACT_VW"
    "TXN_TYP_VW"
    "UNIT_ADVSR_VW"
    "UNIT_ROLE_VW"
    "UNT_VW"
    "WHLSR_HIER_VW"
    "CAL_MO_VW"
    "BI_NFF_FACT_VW"
    "COMM_MO_VW"
    "COMM_WK_VW"
    "GOAL_COMM_FACT_VW"
    "GOAL_NFF_FACT_VW"
    "GOAL_WTD_PREM_CNTR_FACT_VW"
    "GOAL_WTD_PREM_FACT_VW"
    "HIER_RLS_ADVSR_VW"
    "HIER_RLS_AGCY_VW"
    "PROD_GROUP_BRIDGE_VW"
    "WHLSR_VW"
    "ADVSR_AGCY_CNTR_VW"
)

export EXCLUDE_TABLES="$(IFS=, ; echo "${EXCLUDE_TABLES_ARRAY[*]}")"
export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'SLS_AND_DTRB_FACT_VW': {
              'partitionColumn': 'SLS_DTRB_SID',
              'lowerBound':  80000000,
              'upperBound': 300000000,
              'numPartitions': 50
            },
            'NFF_FACT_VW': {
              'partitionColumn': 'NFF_SID',
              'lowerBound': 1,
              'upperBound': 10000000,
              'numPartitions': 50
            }
        }
    }
}
"
