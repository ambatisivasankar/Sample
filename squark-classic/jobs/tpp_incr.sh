
export PROJECT_ID=UWDM
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export CONNECTION_ID=tpp


export INCLUDE_TABLES="app_case,case_status_histry,pclient,prvdr_result_cds,tppdwerrorlog"


export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':
    {
        'app_case': {
            'sql_query': '(SELECT ac.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN app_case ac ON cr.case_id = ac.case_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
            'table_pk': 'case_id'
        },
        'case_status_histry': {
            'sql_query':'(SELECT csh.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN app_case ac ON cr.case_id = ac.case_id  inner join case_status_histry csh on csh.case_id = ac.case_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery'
        },
        'lab_party': {
            'sql_query': '(SELECT * FROM lab_party) as subquery',
            'table_pk': 'party_id'
        },
        'pclient': {
            'sql_query': '( select pc.* FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN app_case ac ON cr.case_id = ac.case_id INNER JOIN PCLIENT PC on PC.PCLI_ID=CR.pcli_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery',
            'table_pk': 'pcli_id'
        },
        'prvdr_result_cds': {
            'sql_query':'(select * From prvdr_result_cds) as subquery'
        },
        'tppdwerrorlog': {
            'sql_query': '(SELECT DISTINCT cast(crr.resp_matched_dtm as date) as resp_matched_dtm ,crr.case_reqt_id,ac.case_app_ref_num,ac.case_id,lrs.summary_id,lp.party_id,pr.prov_result_id,prtrm.tpp_result_id,prc.prov_result_test_desc,pr.provider_testcode,pr.prov_result_val,pr.qualtativ_reslt_val,pr.qualtatv_rslt_val_tc,vm.vendor_mgmt_id,lp.relation_role_cd,PC.PCLI_ID,pc.client_syskey FROM case_reqt cr INNER JOIN case_reqt_response crr ON cr.case_reqt_id = crr.case_reqt_id INNER JOIN vendor_mgmt vm ON crr.vendor_mgmt_id = vm.vendor_mgmt_id INNER JOIN lab_rslt_summary lrs INNER JOIN lab_party lp ON lrs.summary_id = lp.summary_id INNER JOIN provider_results pr ON lp.party_id = pr.party_id INNER JOIN prvdr_result_cds prc ON pr.prov_result_id = prc.prov_result_id INNER JOIN prov_rslt_tpp_res_mappng prtrm ON pr.prov_result_id = prtrm.prov_result_id ON vm.vendor_guid = lrs.tracking_id INNER JOIN app_case ac ON cr.case_id = ac.case_id INNER JOIN PCLIENT PC on PC.PCLI_ID=CR.pcli_id AND ac.case_id NOT IN  (SELECT case_id FROM post_issue) AND match_ind ='''S''' WHERE crr.resp_matched_dtm BETWEEN cast('''$strt_dt''' as date) AND cast('''$end_dt''' as date)) as subquery'
        }

    }
}
"

