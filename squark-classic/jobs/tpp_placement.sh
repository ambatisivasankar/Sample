# Required
export PROJECT_ID=tpp_placement
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=tpp

# optional
export INCLUDE_TABLES="app_case,case_pclient_assoc,pclient"
export INCLUDE_VIEWS=1


export JSON_INFO='
{
    "SUPER_PROJECTION_SETTINGS":{
        "tables": {
            "app_case": {
                "projection_name": "app_case_squark_super_01",
                "order_by_columns": "is_deleted_ind,rstrctd_view_ind,case_stat_cd,case_typ_cd,case_id",
                "segment_by_columns": ""
            }
        }
    }
}
'
