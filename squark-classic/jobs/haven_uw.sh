# Required
export PROJECT_ID=haven_uw
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CONNECTION_ID=haven_uw

# Options
export CHECK_PRIVS=1
export SPARK_MAX_EXECUTORS=60
export SQUARK_METADATA=1
export RUN_LIVE_MAX_LEN_QUERIES=1
export EXCLUDE_SCHEMA='hit'

split_1_include_tables_array=(
  "accident_at_fault"
  "account_authorization"
  "address"
  "agent_default_commission_split"
  "agent_referral_link"
  "app_version"
  "aps_status_event"
  "audit_admin"
  "bluebox_data"
  "code_item"
  "collateral_assignment"
  "conversation"
  "custom_uw_rule_lookup"
  "customer_enquiry"
  "customer_info"
  "dhp_claim_problem"
  "dhp_claim_procedure"
  "dhp_data"
  "diagnosis"
  "diagnosis_short"
  "dimension"
  "driving_records"
  "drug"
  "drug_indication"
  "exchange_1035"
  "family_history"
  "feeds_extract_error_log"
  "financials"
  "flat_extra"
  "follow_up_qa"
  "form"
  "id_check"
  "id_check_data_source"
  "id_followup_action"
  "id_risk_indicator"
  "illustration_request_property"
  "index_schema_history"
  "insurance_quotes"
  "lab_test_result"
  "life_risk_attribute"
  "m3s_rate_class_threshold"
  "mat_result"
  "medical_records"
  "metadata_attribute"
  "mib_code"
  "mib_data_source"
  "mib_person"
  "mm_api_request"
  "mm_api_transaction"
  "owner"
  "party"
  "party_relation"
  "pdf_package"
  "pdf_package_form"
  "pdf_signature_point"
  "phone"
  "policy_account_info"
  "policy_agent"
  "policy_change_history"
  "policy_comment"
  "policy_doc"
  "policy_rider"
  "prescription"
  "price_check_policy"
  "profile"
  "referral"
  "requirement"
  "risk_rule"
  "rx_indication_lookup"
  "stage"
  "telesales_agent"
  "transition"
  "two_factor_auth"
  "uw_risk_group_notes"
  "uw_rule_result"
  "workflow_state_history"
)

s1_include_tables="$(IFS=, ; echo "${split_1_include_tables_array[*]}")"
export SPLIT_1_INCLUDE_TABLES=$s1_include_tables

split_2_include_tables_array=(
  "adv_action_ds_selection"
  "adverse_action"
  "agent_hierarchy"
  "agent_license"
  "agent_profile"
  "analytics_container"
  "analytics_event"
  "app_config"
  "app_log"
  "applicant"
  "application_originator"
  "audit_entity"
  "audit_event"
  "avocation"
  "batch_history"
  "big_game_country"
  "case_action_history"
  "cash_flow"
  "contact_pharmacy"
  "contact_physician"
  "customer"
  "customer_indication"
  "digit"
  "driving_report"
  "dsr"
  "email_lead"
  "esign_doc_metadata"
  "exam_status"
  "existing_policies"
  "family_condition"
  "follow_up"
  "follow_up_answer_ref"
  "follow_up_question_ref"
  "hup_feedback"
  "identity"
  "insurance_coverage"
  "lab_records"
  "lab_test_remark"
  "m3s_rate_class_contribution"
  "metadata_version"
  "mib_comments"
  "mib_data"
  "mib_insurance_activity"
  "mib_planf_policy_ref"
  "mib_planf_task"
  "mm_api_transaction_group"
  "occupation_factors"
  "ops_proxy"
  "other_moving_violation"
  "package"
  "package_form"
  "physician"
  "physician_policy_link"
  "point"
  "points"
  "premium_breakdown"
  "residential_asset"
  "risk_and_probability"
  "risk_classifier_message"
  "risk_classifier_score"
  "risk_classifier_score_data_source"
  "risk_factors"
  "rx_data_source"
  "rx_records"
  "search"
  "settings"
  "standard_driving_violation"
  "surgery"
  "telesales_consent_timestamps"
  "trustee"
  "uw_policy"
  "uw_policy_history_event"
  "uw_rule_lookup"
  "uw_rule_result_attributes"
  "uw_rule_result_desc_value"
  "workflow"
  "workflow_queue_msg"
)

s2_include_tables="$(IFS=,;echo "${split_2_include_tables_array[*]}")"
export SPLIT_2_INCLUDE_TABLES=$s2_include_tables

split_3_include_tables_array=(
  "account"
  "act_pre_requirement"
  "act_requirement"
  "activity"
  "admin"
  "agency"
  "agent"
  "agent_level"
  "application"
  "aps_order"
  "audit"
  "audit_attribute"
  "batch_log"
  "business_purpose_partners"
  "callidus_agent_commission_details"
  "capital_needs"
  "credit_records"
  "driving_message"
  "driving_violation"
  "driving_while_suspended_con"
  "drug_compliance"
  "drug_rule_link"
  "execution"
  "existing_policy"
  "follow_up_comment"
  "form_field"
  "grantor"
  "healthcheck"
  "id_address_history"
  "id_watch_list"
  "insurance_quotes_rider"
  "life_risk"
  "life_risk_result"
  "m3s_rate_class_contribution_value"
  "managed_entity_type"
  "medical_condition"
  "metadata_log"
  "mib_planf_unknown_tracking_ids"
  "mib_review"
  "migration_tracker"
  "mm_api_parameter"
  "mmds_score"
  "mortality_score"
  "normalized_item"
  "pdf_form"
  "pdf_formfield_mapping"
  "personal_history"
  "pharmacy"
  "policy"
  "policy_business_purpose"
  "premium_loan"
  "prescription_fill"
  "previous_company_records"
  "r_x"
  "raw_data"
  "recent_search"
  "reckless_ticket"
  "risk_rule_rating"
  "rx_data"
  "rx_master_drug_lookup"
  "rx_selection"
  "sequence"
  "speeding_ticket"
  "split_party"
  "stage_context"
  "survey_response"
  "telesales_agent_customer"
  "travel_country"
  "trn_requirement"
  "underwriting_decision"
  "universal_beneficiary"
  "user_attributes"
  "uw_metadata_versions"
  "uw_policy_history_attr"
  "uw_policy_rate"
  "visitor"
  "workflow_context"
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

    export INCLUDE_TABLES=HAVEN_UW_INTERACTION_INCR
    export EXCLUDE_TABLES=interaction

    # NO TABLEMAP FOR THE LOAD
    # we _want_ to load in the s3 dir `HAVEN_UW_INTERACTION_INCR` into the table `HAVEN_UW_INTERACTION_INCR`

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
      'interaction': 'HAVEN_UW_INTERACTION_INCR'
  }
}
"
  fi

else
  echo "Setting JSON_INFO for regular job"
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
fi
