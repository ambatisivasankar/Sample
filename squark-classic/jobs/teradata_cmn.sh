#Required
export PROJECT_ID=teradata_cmn
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'


#Optional
export INCLUDE_VIEWS=1
export CONNECTION_ID=teradata_cmn

INCLUDE_TABLES_ARRAY=(
  "AGMT_STUS_CD_LKUP"
  "AGMT_STUS_RSN_CD_LKUP_VW"
  "AGY_CLOSE_MERGE_DATA_VW"
  "COMM_DT_VW"
  "DSTRB_CHNL_CD_VW"
  "DTH_CD_LKUP_VW"
  "GENDER_CD_VW"
  "GOVT_ID_TYPE"
  "GOVT_ID_TYP_VW"
  "MET_AGT_POINTINTIME_VW"
  "MKT_INFO_CD_VW"
  "MKT_TYP_CD_LKUP_VW"
  "PRODUCT_TRANSLATOR_VW"
  "PRTY_RLE_TYP_CD_LKUP_VW"
  "PRTY_TYP_VW"
  "RCOG_PROD_VW"
  "SLS_DTRB_CAL_VW"
  "SRC_DATA_TRNSLT_VW"
  "SRC_SYS_VW"
)

export INCLUDE_TABLES="$(IFS=, ; echo "${INCLUDE_TABLES_ARRAY[*]}")"

export JSON_INFO='
{
    "SUPER_PROJECTION_SETTINGS":{
     "tables": {
         "AGMT_STUS_CD_LKUP": {
             "projection_name": "AGMT_STUS_CD_LKUP_SQUARK",
             "order_by_columns": "AGMT_STUS_CD",
             "segment_by_columns": ""
         },
         "AGMT_STUS_RSN_CD_LKUP_VW": {
             "projection_name": "AGMT_STUS_RSN_CD_LKUP_VW_SQUARK",
             "order_by_columns": "AGMT_STUS_RSN_CD",
             "segment_by_columns": ""
         },
         "AGY_CLOSE_MERGE_DATA_VW": {
             "projection_name": "AGY_CLOSE_MERGE_DATA_VW_SQUARK",
             "order_by_columns": "AGY_ID",
             "segment_by_columns": ""
         },
         "COMM_DT_VW": {
             "projection_name": "COMM_DT_VW_SQUARK",
             "order_by_columns": "COMM_CAL_ID,CAL_DT",
             "segment_by_columns": ""
         },
         "DSTRB_CHNL_CD_VW": {
             "projection_name": "DSTRB_CHNL_CD_VW_SQUARK",
             "order_by_columns": "DSTR_CHNL_CD",
             "segment_by_columns": ""
         },
         "DTH_CD_LKUP_VW": {
             "projection_name": "DTH_CD_LKUP_VW_SQUARK",
             "order_by_columns": "DTH_CD",
             "segment_by_columns": ""
         },
         "GENDER_CD_VW": {
             "projection_name": "GENDER_CD_VW_SQUARK",
             "order_by_columns": "GNDR_CD",
             "segment_by_columns": ""
         },
         "GOVT_ID_TYP_VW": {
             "projection_name": "GOVT_ID_TYP_VW_SQUARK",
             "order_by_columns": "GOVT_ID_TYP",
             "segment_by_columns": ""
         },
         "MET_AGT_POINTINTIME_VW": {
             "projection_name": "MET_AGT_POINTINTIME_VW_SQUARK",
             "order_by_columns": "BUSINESS_PARTNER_ID",
             "segment_by_columns": ""
         },
         "MKT_INFO_CD_VW": {
             "projection_name": "MKT_INFO_CD_VW_SQUARK",
             "order_by_columns": "MKT_INFO_CD",
             "segment_by_columns": ""
         },
         "MKT_TYP_CD_LKUP_VW": {
             "projection_name": "MKT_TYP_CD_LKUP_VW_SQUARK",
             "order_by_columns": "MKT_TYP_CD",
             "segment_by_columns": ""
         },
         "PRODUCT_TRANSLATOR_VW": {
             "projection_name": "PRODUCT_TRANSLATOR_VW_SQUARK",
             "order_by_columns": "PROD_ID",
             "segment_by_columns": ""
         },
         "PRTY_RLE_TYP_CD_LKUP_VW": {
             "projection_name": "PRTY_RLE_TYP_CD_LKUP_VW_SQUARK",
             "order_by_columns": "PRTY_RLE",
             "segment_by_columns": ""
         },
         "PRTY_TYP_VW": {
             "projection_name": "PRTY_TYP_VW_SQUARK",
             "order_by_columns": "PRTY_TYP_CDE",
             "segment_by_columns": ""
         },
         "RCOG_PROD_VW": {
             "projection_name": "RCOG_PROD_VW_SQUARK",
             "order_by_columns": "RCOG_PROD_ID",
             "segment_by_columns": ""
         },
         "SLS_DTRB_CAL_VW": {
             "projection_name": "SLS_DTRB_CAL_VW_SQUARK",
             "order_by_columns": "CAL_DT",
             "segment_by_columns": ""
         },
         "SRC_DATA_TRNSLT_VW": {
             "projection_name": "SRC_DATA_TRNSLT_VW_SQUARK",
             "order_by_columns": "SRC_FLD_VAL,SRC_CDE,SRC_FLD_NM,TRNSLT_FLD_NM,TRNSLT_FLD_VAL",
             "segment_by_columns": ""
         },
         "SRC_SYS_VW": {
             "projection_name": "SRC_SYS_VW_SQUARK",
             "order_by_columns": "SRC_SYS_ID",
             "segment_by_columns": ""
         }
     }
  }
}
'
