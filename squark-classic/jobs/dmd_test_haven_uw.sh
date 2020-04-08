# Required
# This job is designed to non-destructively load data into dmd_test
# MAKE SURE TO RUN LAUNCH SQUARK JOB WITH --skip-schema
# MAKE SURE TO SET schema_name=dmd_test
export PROJECT_ID=dmd_test
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CONNECTION_ID=haven_uw

# Options
export CHECK_PRIVS=1
export SPARK_MAX_EXECUTORS=60
export SQUARK_METADATA=1
export RUN_LIVE_MAX_LEN_QUERIES=1
export INCLUDE_TABLES=interaction
export SPARK_EXECUTOR_MEMORY="20G"

export JSON_INFO="
{
  'PARTITION_INFO':{
    'tables': {
      'interaction': {
        'partitionColumn': 'MOD(TO_CHAR(\\\"createdTime\\\" , '''MMSS''')::integer, 460)',
        'lowerBound': 0,
        'upperBound': 460,
        'numPartitions': 460
      }
    }
  },
  'TABLE_MAP':{
    'interaction': 'haven_uw_interaction'
  }
}
"
