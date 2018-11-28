export PROJECT_ID=tpp
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
#export EXCLUDE_TABLES="letter_actnble_ecg_doctor,letter_ioli_cover_sheet"
export INCLUDE_TABLES="MIB_FORM_RESP,MIB_reqst_rpt_code,app_case,base_prod,case_clnt_prob,case_methodology,case_owner,case_pclient_assoc,case_questions,case_reqt,case_reqt_response,case_status_histry,caseuser,ctry,events,mib_resp_codes,mib_response,mib_response_person,mm_app_case,mm_case_agent,pclient,pclient_address,plan_pref_criteria,pref_criteria,problem,prov_rslt_tpp_res_mappng,prvdr_result_cds,question,questionnaire_selection,requirement,tpp_results,users,vendor_mgmt,mm_case_audit_fld,mm_case_pclient_assoc,case_premium_info,case_status,uw_rating,lab_rx_request,lab_rx_prescription_drug,case_reqt_status,MIB_code,MIB_combined_codes"
#2017.07.10, we don't know why SKIP_ERRORS was enabled in the first place but disabling now
#export SKIP_ERRORS=1
export INCLUDE_VIEWS=1
export CONNECTION_ID=tpp
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
