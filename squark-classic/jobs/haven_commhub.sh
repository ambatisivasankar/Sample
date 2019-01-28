export PROJECT_ID=haven_commhub
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven_commhub
export SQUARK_METADATA=1
export CONVERT_ARRAYS_TO_STRING=1

# md_email_content_raw, remove any letters (g=global, i=case-insensitive), return first two digits, which should be implicitly cast as INT for comparison purposes
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'md_email_content_raw': {
              'partitionColumn': 'LEFT(regexp_replace(id, '''[a-z]''', '''''', '''gi'''), 2)',
              'lowerBound': 0,
              'upperBound': 99,
              'numPartitions': 99
            },
            'zd_ticket': {
              'partitionColumn': 'id',
              'lowerBound': 27000,
              'upperBound': 600000,
              'numPartitions': 50
            },
            'zd_comment_raw': {
              'partitionColumn': 'id',
              'lowerBound': 27000,
              'upperBound': 600000,
              'numPartitions': 50
            }
        }
   }
}
"