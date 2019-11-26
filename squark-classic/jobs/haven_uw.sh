# Dummy file required by load_wh.sh
export PROJECT_ID=haven_uw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
export CONNECTION_ID=haven_uw
export SPARK_MAX_EXECUTORS=60
export SQUARK_METADATA=1
export RUN_LIVE_MAX_LEN_QUERIES=1
export EXCLUDE_SCHEMA='hit'

# SECONDs returned as fractional values, e.g. 59.565992, so 60 partitions works
# 2018.10.11, hard-code the SELECT for policy_doc in order to skip the BLOBy doc column that we don't need and slows down the ingestion
#   partitioning becomes unnecessary but the workflow is much more fragile, need to sync the SELECT w/any policy_doc DDL changes
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
            'act_requirement': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'act_pre_requirement': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'activity': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'address': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
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
            'cash_flow': {
              'partitionColumn': 'MOD(DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))::integer, 5)',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            },
            'contact_physician': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'conversation': {
              'partitionColumn': 'MOD(DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))::integer, 5)',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            },
            'credit_records': {
              'partitionColumn': 'MOD(TO_CHAR(\\\"createdTime\\\" , '''MMSS''')::integer, 120)',
              'lowerBound': 0,
              'upperBound': 120,
              'numPartitions': 120
            },
            'customer': {
              'partitionColumn': 'MOD(DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))::integer, 5)',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            },
            'dimension': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'driving_records': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'drug': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'drug_indication': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'drug_rule_link': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'execution': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'interaction': {
              'partitionColumn': 'MOD(TO_CHAR(\\\"createdTime\\\" , '''MMSS''')::integer, 460)',
              'lowerBound': 0,
              'upperBound': 460,
              'numPartitions': 460
            },
            'life_risk': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'life_risk_attribute': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'old_interaction': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'party': {
              'partitionColumn': 'MOD(DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))::integer, 5)',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
            },
            'policy_doc_SKIP': {
              'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 59
            },
            'pharmacy': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'phone': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'physician': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'prescription': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'prescription_fill': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'raw_data': {
              'partitionColumn': 'MOD(TO_CHAR(\\\"createdTime\\\" , '''MMSS''')::integer, 120)',
              'lowerBound': 0,
              'upperBound': 120,
              'numPartitions': 120
            },
            'requirement': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'risk_rule': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'risk_rule_rating': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'risk_classifier_score': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'rx_records': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'transition': {
              'partitionColumn': 'DATE_PART('''MONTH''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 12,
              'numPartitions': 12
            },
            'trn_requirement': {
              'partitionColumn': 'DATE_PART('''DAY''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 1,
              'upperBound': 31,
              'numPartitions': 31
            },
            'workflow_history': {
              'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
              'lowerBound': 0,
              'upperBound': 59,
              'numPartitions': 60
            },
            'uw_policy_history_attr': {
              'partitionColumn': 'MOD(DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))::integer, 5)',
              'lowerBound': 0,
              'upperBound': 5,
              'numPartitions': 5
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


#export STATS_CONFIG="
#{
#    'profiles': {
#        'numeric': ['max','min','mean','countDistinct','count_null'],
#        'string': ['max', 'min','countDistinct','count_null'],
#        'datetype': ['max', 'min','countDistinct','count_null']
#    },
#    'field_types': {
#        'NumericType': 'numeric',
#        'StringType': 'string',
#        'DateType': 'datetype',
#        'TimestampType': 'datetype'
#    }
#}
#"
