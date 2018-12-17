export PROJECT_ID=ddnb1
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=ddnb1

export SPARK_MAX_EXECUTORS=15

export INCLUDE_TABLES_ARRAY=(
    "MEDICAL"
    "OCCUPATION_CODES"
    "OCC_CLS_CLSF"
    "EXCLUSIONS"
    "EXCLUSION_TEXT"
    "FINAL_ACTION"
    "FINAL_ACTION_HISTORY"
    "SUBSTANDARD_RATING"
    "INBASKET_EXT"
    "INSURED"
    "DCLN_CDE"
    "DCLN_RSN"
    "MIB_AUDIT"
    "MIB_FOLLOWUP_PENDING"
    "MIB_TRNSLT_BLOD_PRSR_CDE"
    "MIB_TRNSLT_COMB_CDE"
    "MIB_TRNSLT_MIB_CDE"
    "MIB_TRNSLT_MOD_DEG_CDE"
    "MIB_TRNSLT_SITE_CDE"
    "MIB_TRNSLT_SPCL_CDE"
    "MIB_REPLY"
    "MIB_TRNSLT_SRC_CDE"
    "MIB_TRNSLT_TM_CDE"
    "MIB_REPORTED"
    "MIB_REPLY_DATA"
)

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"