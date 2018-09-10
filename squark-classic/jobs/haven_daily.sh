# Dummy file required by load_wh.sh
export PROJECT_ID=haven_daily
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven
export SPARK_MAX_EXECUTORS=60
export SQUARK_METADATA=1
export SKIP_SOURCE_ROW_COUNT=1

# 2018.07.06, application2 & workflow_history2 are in here only to test deletes, we create via SELECT INTO on RDS and then we own
# SECONDs returned as fractional values, e.g. 59.565992, so 60 partitions works
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'analytics_container': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'analytics_event': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'application': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'application2': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'interaction': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'old_interaction': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'policy_doc': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'raw_data': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'workflow_history': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'workflow_history2': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'uw_rule_result': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            },
            'uw_rule_result_desc_value': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60,
              'is_incremental': '1',
              'base_schema_name': 'haven_weekly',
              'last_updated_column_name': 'lastUpdatedTime',
              'pkid_column_name': '_id'
            }
        }
   }
}
"


