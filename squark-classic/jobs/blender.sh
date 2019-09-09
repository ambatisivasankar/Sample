export PROJECT_ID=blender
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=blender
export SQUARK_METADATA=1

export JSON_INFO='
{
    "SUPER_PROJECTION_SETTINGS":{
        "tables": {
            "NOTIF": {
                "projection_name": "NOTIF_SQUARK",
                "order_by_columns": "NOTIF_TYP_CDE,STUS_CDE,POL_NR,NOTIF_ID",
                "segment_by_columns": "NOTIF_ID"
            }
        }
    },
    "PARTITION_INFO":{
        "tables": {
            "FNL_POL_RULE_REQ": {
              "partitionColumn": "RULE_REQ_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "NOTIF": {
              "partitionColumn": "NOTIF_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "RULE_REQ": {
              "partitionColumn": "RULE_REQ_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            },
            "UW_POL": {
              "partitionColumn": "UW_POL_ID % 100",
              "lowerBound": 0,
              "upperBound": 100,
              "numPartitions": 100
            }
        }
   }
}
'
