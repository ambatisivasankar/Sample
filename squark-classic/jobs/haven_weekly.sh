# Dummy file required by load_wh.sh
export PROJECT_ID=haven_weekly
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven
export SPARK_MAX_EXECUTORS=60
export SQUARK_METADATA=1
export SKIP_SOURCE_ROW_COUNT=1

# SECONDs returned as fractional values, e.g. 59.565992, so 60 partitions works
# 2018.10.12, hard-code the SELECT for policy_doc in order to skip the BLOBy doc column that we don't need and slows down the ingestion
#   partitioning/incremental becomes unnecessary but the workflow is much more fragile, need to sync the SELECT w/any policy_doc DDL changes
export JSON_INFO="
{
	'SAVE_TABLE_SQL_SUBQUERY':{
      'schema': 'dbo',
      'table_queries': {
            'policy_doc': '(SELECT \\\"_id\\\",\\\"_template\\\",\\\"__version__\\\",NULL AS doc,\\\"type\\\",\\\"docType\\\",\\\"docSource\\\",\\\"appType\\\",\\\"subType\\\",\\\"policyId\\\",\\\"name\\\",\\\"date\\\",\\\"uploadedBy\\\",\\\"language\\\",\\\"roles\\\",\\\"order\\\",\\\"follow_up_qa_id\\\",\\\"createdTime\\\",\\\"lastUpdatedTime\\\",\\\"status\\\" FROM policy_doc) as subquery'
        }
    },
    'PARTITION_INFO':{
        'tables': {
            'analytics_container': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'analytics_event': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'application': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'application2': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'drug_indication': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'interaction': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'old_interaction': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'policy_doc_SKIP': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'raw_data': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'workflow_history': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'workflow_history2': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'uw_rule_result': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'uw_rule_result_desc_value': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            }
        }
   }
}
"


