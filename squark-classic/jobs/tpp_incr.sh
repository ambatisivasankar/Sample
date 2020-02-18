
export PROJECT_ID=squark_staging
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=tpp

# note that in the 'prov_rslt_tpp_res_mappng' table below is actual the tpp_fact_table. Reason of using the prov_rslt_tpp_res_mappng's name
# is due to the limitation of how current squark work - only create table that exists in the source db
export INCLUDE_TABLES="tppdwerrorlog,app_case,pclient,prvdr_result_cds,case_status_histry"


# 2018.04.18, partition results on AGMT_HIST_VW pretty bad, AGREEMENT_ID has bimodal-ish dist for this date range
#   in order to use value in the subfilter, double-quote all innards of the JSON

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':
    {
        'tppdwerrorlog': {
            'sql_query': '(SELECT DISTINCT cast(crr.resp_matched_dtm as date) as resp_matched_dtm ,crr.case_reqt_id,ac.case_app_ref_num,ac.case_id,lrs.summary_id,lp.party_id,pr.prov_result_id,prtrm.tpp_result_id,prc.prov_result_test_desc,pr.provider_testcode,pr.prov_result_val,pr.qualtativ_reslt_val,pr.qualtatv_rslt_val_tc,vm.vendor_mgmt_id,lp.relation_role_cd,PC.PCLI_ID,pc.client_syskey FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN vendor_mgmt vm ON crr.vendor_mgmt_id = vm.vendor_mgmt_id INNER JOIN lab_rslt_summary lrs INNER JOIN lab_party lp ON lrs.summary_id = lp.summary_id INNER JOIN provider_results pr ON lp.party_id = pr.party_id INNER JOIN prvdr_result_cds prc ON pr.prov_result_id = prc.prov_result_id INNER JOIN prov_rslt_tpp_res_mappng prtrm ON pr.prov_result_id = prtrm.prov_result_id ON vm.vendor_guid = lrs.tracking_id INNER JOIN app_case ac ON cr.case_id = ac.case_id INNER JOIN PCLIENT PC on PC.PCLI_ID=CR.pcli_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery'
        },
        'app_case': {
            'sql_query': '(SELECT ac.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN app_case ac ON cr.case_id = ac.case_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
            'table_pk': 'case_id'
        },
        'lab_rslt_summary': {
            'sql_query': '(SELECT lrs.* FROM lab_rslt_summary lrs WHERE cast(lst_updt_dtm as date) BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt'''  as date) union select distinct lrs.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN vendor_mgmt vm ON crr.vendor_mgmt_id = vm.vendor_mgmt_id INNER JOIN lab_rslt_summary lrs INNER JOIN lab_party lp ON lrs.summary_id = lp.summary_id INNER JOIN provider_results pr ON lp.party_id = pr.party_id INNER JOIN prvdr_result_cds prc ON pr.prov_result_id = prc.prov_result_id INNER JOIN prov_rslt_tpp_res_mappng prtrm ON pr.prov_result_id = prtrm.prov_result_id ON vm.vendor_guid = lrs.tracking_id INNER JOIN app_case ac ON cr.case_id = ac.case_id INNER JOIN PCLIENT PC on PC.PCLI_ID=CR.pcli_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt'''  as date)) as subquery',
            'table_pk': 'summary_id'
        },
        'lab_party': {
            'sql_query': '(SELECT * FROM lab_party) as subquery',
            'table_pk': 'party_id'
        },
        'pclient': {
            'sql_query': '( select pc.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN app_case ac ON cr.case_id = ac.case_id INNER JOIN PCLIENT PC on PC.PCLI_ID=CR.pcli_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
            'table_pk': 'pcli_id'
        },
        'lab_policy': {
            'sql_query':'(select * From lab_policy) as subquery',
            'table_pk': 'summary_id'
        },
        'prvdr_result_cds': {
            'sql_query':'(select * From prvdr_result_cds) as subquery'
        },
        'prov_rslt_tpp_res_mappng': {
            'sql_query':'(select * From prov_rslt_tpp_res_mappng) as subquery'
        },
        'case_status_histry': {
            'sql_query':'(SELECT csh.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN app_case ac ON cr.case_id = ac.case_id  inner join case_status_histry csh on csh.case_id = ac.case_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery'
        }

    }
}
"

#
#        'lab_party': {
#            'sql_query': '(SELECT * FROM lab_party WHERE cast(lst_updt_dtm as date) BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
#            'table_pk': 'party_id'
#        },
