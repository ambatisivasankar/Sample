# Required
export PROJECT_ID=haven_commhub
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CONNECTION_ID=haven_commhub

# Optional
export SQUARK_METADATA=1
export CHECK_PRIVS=1
export CONVERT_ARRAYS_TO_STRING=1

# Notes
# md_email_content_raw: remove any letters (g=global, i=case-insensitive), return first two digits and cast as INT
export JSON_INFO="
{
    'PARTITION_INFO':{
        'tables': {
            'email_addr': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"send_datetime\\\"::timestamp , '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'email_hub': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"send_datetime\\\"::timestamp, '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'mandrill_event_flat': {
              'partitionColumn': 'MOD(msg_ts::integer, 20)',
              'lowerBound': 0,
              'upperBound': 20,
              'numPartitions': 20
            },
            'md_email_content_raw': {
              'partitionColumn': 'CAST(LEFT(regexp_replace(id, '''[a-z]''', '''''', '''gi'''), 2) AS INTEGER)',
              'lowerBound': 0,
              'upperBound': 99,
              'numPartitions': 99
            },
            'zd_ticket': {
              'partitionColumn': 'MOD(id::integer, 100)',
              'lowerBound': 0,
              'upperBound': 100,
              'numPartitions': 100
            },
            'zd_comment_raw': {
              'partitionColumn': 'MOD(id, 50)',
              'lowerBound': 0,
              'upperBound': 50,
              'numPartitions': 50
            },
            'zd_ticket_raw': {
              'partitionColumn': 'MOD(id, 100)',
              'lowerBound': 0,
              'upperBound': 100,
              'numPartitions': 100
            }
        }
   }
}
"