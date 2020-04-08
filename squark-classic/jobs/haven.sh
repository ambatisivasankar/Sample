# Required
export PROJECT_ID=haven
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CONNECTION_ID=haven

# Options
export CHECK_PRIVS=1
export SPARK_MAX_EXECUTORS=60
export SQUARK_METADATA=1
export RUN_LIVE_MAX_LEN_QUERIES=1

split_1_include_tables_array=(
  "accident_at_fault"
  "agent"
  "agent_license"
  "analytics_container"
  "analytics_event"
  "app_config"
  "applicant"
  "application"
  "audit"
  "audit_attribute"
  "audit_event"
  "batch_log"
  "case_action_history"
  "custom_uw_rule_lookup"
  "customer"
  "customer_enquiry"
  "diagnosis"
  "diagnosis_short"
  "driving_records"
  "drug"
  "drug_rule_link"
  "esign_doc_metadata"
  "execution"
  "family_condition"
  "family_history"
  "feeds_extract_error_log"
  "financials"
  "follow_up_comment"
  "follow_up_question_ref"
  "grantor"
  "healthcheck"
  "id_address_history"
  "id_check_data_source"
  "id_watch_list"
  "identity"
  "insurance_quotes"
  "lab_test_result"
  "m3s_rate_class_contribution"
  "metadata_attribute"
  "mib_code"
  "mib_data_source"
  "mib_person"
  "mm_api_parameter"
  "mm_api_request"
  "mmds_score"
  "mortality_score"
  "owner"
  "pdf_form"
  "pdf_formfield_mapping"
  "pdf_package"
  "pdf_signature_point"
  "policy_change_history"
  "prescription"
  "prescription_fill"
  "profile"
  "reckless_ticket"
  "requirement"
  "risk_classifier_score"
  "risk_rule"
  "rx_data"
  "settings"
  "speeding_ticket"
  "stage"
  "telesales_agent"
  "trn_requirement"
  "universal_beneficiary"
  "uw_policy"
  "uw_policy_history_event"
  "uw_rule_result_attributes"
  "visitor"
  "workflow_queue_msg"

)

s1_include_tables="$(
  IFS=,
  echo "${split_1_include_tables_array[*]}"
)"
export SPLIT_1_INCLUDE_TABLES=$s1_include_tables

split_2_include_tables_array=(
  "account_authorization"
  "act_requirement"
  "admin"
  "adverse_action"
  "agent_default_commission_split"
  "agent_level"
  "agent_profile"
  "app_log"
  "application_originator"
  "aps_order"
  "audit_admin"
  "avocation"
  "batch_history"
  "contact_physician"
  "digit"
  "dimension"
  "driving_message"
  "driving_report"
  "driving_while_suspended_con"
  "drug_indication"
  "dsr"
  "email_lead"
  "existing_policies"
  "flat_extra"
  "form"
  "form_field"
  "id_check"
  "id_followup_action"
  "id_risk_indicator"
  "insurance_coverage"
  "insurance_quotes_rider"
  "lab_records"
  "mat_result"
  "medical_condition"
  "medical_records"
  "mib_data"
  "mib_insurance_activity"
  "mm_api_transaction"
  "occupation_factors"
  "ops_proxy"
  "other_moving_violation"
  "party"
  "pdf_package_form"
  "phone"
  "physician"
  "points"
  "policy"
  "policy_account_info"
  "policy_business_purpose"
  "policy_comment"
  "policy_doc"
  "premium_breakdown"
  "previous_company_records"
  "price_check_policy"
  "risk_and_probability"
  "risk_classifier_message"
  "risk_factors"
  "risk_rule_rating"
  "rx_indication_lookup"
  "rx_master_drug_lookup"
  "rx_records"
  "search"
  "split_party"
  "telesales_agent_customer"
  "transition"
  "travel_country"
  "uw_metadata_versions"
  "uw_rule_result"
  "workflow_context"
  "workflow_state_history"

)

s2_include_tables="$(
  IFS=,
  echo "${split_2_include_tables_array[*]}"
)"
export SPLIT_2_INCLUDE_TABLES=$s2_include_tables

split_3_include_tables_array=(
  "account"
  "act_pre_requirement"
  "activity"
  "address"
  "adv_action_ds_selection"
  "agency"
  "agent_hierarchy"
  "agent_proxy"
  "agent_referral_link"
  "app_version"
  "audit_entity"
  "big_game_country"
  "business_purpose_partners"
  "callidus_agent_commission_details"
  "capital_needs"
  "cash_flow"
  "collateral_assignment"
  "contact_pharmacy"
  "conversation"
  "credit_records"
  "customer_indication"
  "driving_violation"
  "drug_compliance"
  "exchange_1035"
  "existing_policy"
  "follow_up"
  "follow_up_answer_ref"
  "follow_up_qa"
  "illustration_request_property"
  "index_schema_history"
  "life_risk"
  "life_risk_attribute"
  "life_risk_result"
  "m3s_rate_class_contribution_value"
  "m3s_rate_class_threshold"
  "managed_entity_type"
  "metadata_log"
  "metadata_version"
  "mm_api_transaction_group"
  "package"
  "package_form"
  "party_relation"
  "personal_history"
  "pharmacy"
  "physician_policy_link"
  "point"
  "policy_agent"
  "policy_rider"
  "premium_loan"
  "r_x"
  "raw_data"
  "referral"
  "residential_asset"
  "risk_classifier_score_data_source"
  "rx_data_source"
  "rx_selection"
  "sequence"
  "stage_context"
  "standard_driving_violation"
  "surgery"
  "telesales_consent_timestamps"
  "trustee"
  "two_factor_auth"
  "uw_policy_history_attr"
  "uw_policy_rate"
  "uw_risk_group_notes"
  "uw_rule_lookup"
  "uw_rule_result_desc_value"
  "workflow"
  "workflow_settings"
)

s3_include_tables="$(IFS=, ; echo "${split_3_include_tables_array[*]}")"
export SPLIT_3_INCLUDE_TABLES=$s3_include_tables

export SPLIT_4_EXCLUDE_TABLES="${SPLIT_1_INCLUDE_TABLES},${SPLIT_2_INCLUDE_TABLES},${SPLIT_3_INCLUDE_TABLES},interaction"


# IS_INCREMENTAL_JOB and IS_INCREMENTAL_LOAD (set to true jenkins, defaults to False if not set)
# 0 = False, 1 = True

IS_INCREMENTAL_JOB=${IS_INCREMENTAL_JOB:-0}
if [ "${IS_INCREMENTAL_JOB}" -eq 1 ]; then
  echo "Setting variables for incremental job"

  IS_INCREMENTAL_LOAD=${IS_INCREMENTAL_LOAD:-0}
  if [ "${IS_INCREMENTAL_LOAD}" -eq 1 ]; then
    echo "Setting include / exclude for incremental load"

    export INCLUDE_TABLES=HAVEN_INTERACTION_INCR
    export EXCLUDE_TABLES=interaction

    # NO TABLEMAP FOR THE LOAD
    # we _want_ to load in the s3 dir `HAVEN_INTERACTION_INCR` into the table `HAVEN_INTERACTION_INCR`

  else
    echo "Setting include / exclude / JSON_INFO for incremental export"
    export INCLUDE_TABLES=interaction
    # DELTA_RANGE = Number of Dates to use for Incremetal/Delta query. (set in jenkins, defaults to 2 if not set)
    delta_range=${DELTA_RANGE:-2}
    current_date=$(date '+%Y-%m-%d')

    # end_dt = If END_DT is set in jenkins then then END_DT, otherwise todays date
    end_dt=${END_DT:-${current_date}}
    export JSON_INFO="
{
  'SAVE_TABLE_SQL_SUBQUERY':{
    'interaction': {
      'sql_query': '(SELECT * FROM \\\"interaction\\\" WHERE (\\\"createdTime\\\" BETWEEN '''${end_dt}'''::date - ${delta_range} AND '''${end_dt}'''::date + 1) OR (\\\"lastUpdatedTime\\\" BETWEEN '''${end_dt}'''::date - ${delta_range} AND '''${end_dt}'''::date + 1)) as subquery',
      'partitionColumn': 'MOD(TO_CHAR(\\\"createdTime\\\" , '''SS''')::integer, 60)',
      'lowerBound': 0,
      'upperBound': 59,
      'numPartitions': 60
    }
  },
  'TABLE_MAP':{
      'interaction': 'HAVEN_INTERACTION_INCR'
  }
}
"
  fi
else
  echo "Setting JSON_INFO for regular load"
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
      'act_pre_requirement': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'act_requirement': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'activity': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'address': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
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
      'audit_attribute': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'audit_entity': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'audit_event': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'cash_flow': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 60
      },
      'contact_physician': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 60
      },
      'credit_records': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 60
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
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 60
      },
      'drug_indication': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 60
      },
      'interaction': {
        'partitionColumn': 'MOD(TO_CHAR(\\\"createdTime\\\" , '''MMSS''')::integer, 240)',
        'lowerBound': 0,
        'upperBound': 240,
        'numPartitions': 240
      },
      'old_interaction': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'life_risk_attribute': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'party': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'personal_history': {
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
      'prescription_fill': {
        'partitionColumn': 'DATE_PART('''MINUTE''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'raw_data': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'requirement': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'risk_classifier_score': {
        'partitionColumn': 'DATE_PART('''SECOND''', COALESCE(\\\"createdTime\\\", '''1970-01-01T00:00:00'''::timestamp))',
        'lowerBound': 0,
        'upperBound': 59,
        'numPartitions': 59
      },
      'trn_requirement': {
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
fi
