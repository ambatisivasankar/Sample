export PROJECT_ID=teradata_di_recovery
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='DBO_DIBS_CLAIM_VW,DBO_WORK_OBJECT_VW,DBO_WOB_TIME_SERIES_VW,DBO_TREX_USERS_VW,DBO_TREX_WORKFLOW_VW,DBO_CONTENT_VW'
export CONNECTION_ID=teradata_prod_dma


export SPARK_MAX_EXECUTORS=10
export JSON_INFO='{
	"PARTITION_INFO": {
		"tables": {
			"DBO_WOB_TIME_SERIES_VW": {
				"partitionColumn": "RWorkflowTimeSeriesID",
				"lowerBound": 0,
				"upperBound": 21000000,
				"numPartitions": 50
			}
		}
	}
}'
