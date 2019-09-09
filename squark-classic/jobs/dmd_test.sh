
export PROJECT_ID=squark_staging
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_CMN_VW'
export CONNECTION_ID=teradata
export SKIP_SOURCE_ROW_COUNT=0
export SPARK_MAX_EXECUTORS=10

set -x
date -d '-1 day' '+%Y-%m-%d'
echo `date -d '-2 day' '+%Y-%m-%d'`
export strt_dt=`date -d '-1 day' '+%Y-%m-%d'`
export end_dt=`date -d '-0 day' '+%Y-%m-%d'`


echo "Start dt: " $strt_dt
echo "End Dt: " $end_dt


# 2018.04.18, partition results on AGMT_HIST_VW pretty bad, AGREEMENT_ID has bimodal-ish dist for this date range
#   in order to use value in the subfilter, double-quote all innards of the JSON

export JSON_INFO="
{
    'SAVE_TABLE_SQL_SUBQUERY':{
        'AGMT_CMN_VW': {
            'sql_query': '(SELECT * FROM AGMT_CMN_VW  where AGREEMENT_ID in (select AGREEMENT_ID from AGMT_HIST_VW  WHERE CAST(AGMT_HIST_FR_DT as DATE) between cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date) group by 1)) as subquery',
            'numPartitions':10,
            'partitionColumn': '(AGREEMENT_ID Mod 31)',
            'lowerBound': 0,
            'upperBound': 30,
            'table_pk':'AGREEMENT_ID'                 
            
        },
        'AGMT_ADDL_DATA_VW': {
            'sql_query': '(SELECT * FROM AGMT_ADDL_DATA_VW where TRANS_DT BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date)) as subquery' ,
            'partitionColumn': 'AGREEMENT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10 ,
            'table_pk':'AGREEMENT_ID'                                    
        },
        'AGMT_CVG_CMN_VW': {
            'sql_query': '(SELECT CMN.*,COALESCE(CMN.AGREEMENT_ID,A_ID) A_ID,COALESCE(CMN.COVERAGE_KEY,C_K) C_K FROM AGMT_CVG_CMN_VW CMN RIGHT OUTER JOIN (SELECT AGREEMENT_ID A_ID,COVERAGE_KEY C_K FROM AGMT_CVG_HIST_VW WHERE CAST(AGMT_CVG_HIST_FR_DT AS DATE) BETWEEN cast('''$strt_dt''' as date)-5 AND cast('''$end_dt''' as date) OR CAST(AGMT_CVG_HIST_TO_DT AS DATE) BETWEEN cast('''$strt_dt''' as date)-5 AND cast('''$end_dt''' as date) GROUP BY 1,2) HIST ON CMN.AGREEMENT_ID=HIST.A_ID AND CMN.COVERAGE_KEY=HIST.C_K) as subquery',
            'partitionColumn': 'A_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10,
            'table_pk':'AGREEMENT_ID,COVERAGE_KEY'  
        },
        'AGMT_FND_CMN_VW': {
            'sql_query': '(SELECT * FROM AGMT_FND_CMN_VW ) as subquery',
            'partitionColumn': 'AGREEMENT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 10,
            'numPartitions': 10                    
        },
        'AGMT_FND_VAL_CMN_VW': {
            'sql_query': '(SELECT * FROM AGMT_FND_VAL_CMN_VW ) as subquery' ,
            'partitionColumn': 'AGREEMENT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10                    
        },
        'AGMT_GRP_CMN_VW': {
            'sql_query': '(SELECT GRP.*,D_KY,D_SRC,G_FR,G_TO FROM AGMT_GRP_CMN_VW GRP right outer JOIN (select GROUPING_KEY D_KY,GROUP_DATA_SRC D_SRC,GROUPING_KEY_FR_DT G_FR,GROUPING_KEY_TO_DT AS G_TO FROM AGMT_GRP_HIST_VW WHERE GROUP_TRANS_DT BETWEEN CAST('''$strt_dt''' AS DATE)-5 AND CAST('''$end_dt''' AS DATE) GROUP BY 1,2,3,4) HIST ON GROUPING_KEY=D_KY AND GROUP_DATA_SRC=D_SRC AND GROUPING_KEY_FR_DT=G_FR) as subquery',
            'table_pk':'GROUPING_KEY, GROUP_DATA_SRC, CAST(GROUPING_KEY_FR_DT AS DATE)'  

        },
        'AGMT_LOAN_CMN_VW': {
            'sql_query': '(select * from AGMT_LOAN_CMN_VW WHERE (AGREEMENT_ID, AGREEMENT_SOURCE_CD ,HLDG_KEY_PFX ,HLDG_KEY ,HLDG_KEY_SFX ) IN (select AGREEMENT_ID, AGREEMENT_SOURCE_CD ,HLDG_KEY_PFX ,HLDG_KEY ,HLDG_KEY_SFX from  AGMT_LOAN_HIST_VW WHERE CAST(AGMT_LOAN_HIST_FR_DT as date) between cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date) GROUP BY 1,2,3,4,5)) as subquery' ,
            'partitionColumn': 'AGREEMENT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10 ,
            'table_pk':'AGREEMENT_ID, AGREEMENT_SOURCE_CD ,HLDG_KEY_PFX ,HLDG_KEY ,HLDG_KEY_SFX'                     
        },
        'AGMT_UWRT_CMN_VW': {
            'sql_query': '(select * from AGMT_UWRT_CMN_VW WHERE (AGREEMENT_ID,AGREEMENT_SOURCE_CD,HLDG_KEY_PFX,HLDG_KEY,HLDG_KEY_SFX ) IN (select AGREEMENT_ID,AGREEMENT_SOURCE_CD,HLDG_KEY_PFX,HLDG_KEY,HLDG_KEY_SFX  from AGMT_UWRT_HIST_VW  where AGMT_UWRT_HIST_FR_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) GROUP BY 1,2,3,4,5)) as subquery',
            'partitionColumn': 'AGREEMENT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10,
            'table_pk':'AGREEMENT_ID, AGREEMENT_SOURCE_CD ,HLDG_KEY_PFX ,HLDG_KEY ,HLDG_KEY_SFX'                         
        },
        'AGMT_VAL_CMN_VW': {
            'sql_query': '(SELECT * FROM AGMT_VAL_CMN_VW where TRANS_DT BETWEEN cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date)) as subquery',
            'partitionColumn': 'AGREEMENT_ID MOD 20',
            'lowerBound': 0,
            'upperBound': 19,
            'numPartitions': 20,
            'table_pk':'AGREEMENT_ID'                                       
        },
        'AGMT_WARNING_INFO_CMN_VW': {
            'sql_query': '(SELECT * FROM AGMT_WARNING_INFO_CMN_VW ) as subquery',
            'table_pk':'AGREEMENT_ID, MSG_WORD_DESC'
        }, 
        'BENE_DATA_CMN_VW': {
            'sql_query': '(select CMN.*,A_ID,SRC_CD,K_PF,KY,K_SF From BENE_DATA_CMN_VW cmn right outer join (select AGREEMENT_ID A_ID,AGREEMENT_SOURCE_CD SRC_CD,HLDG_KEY_PFX K_PF,HLDG_KEY KY,HLDG_KEY_SFX K_SF from BENE_DATA_HIST_VW where BENE_HIST_FR_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) OR BENE_HIST_TO_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) or AGMT_DATA_FR_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) GROUP BY 1,2,3,4,5)hist   on hist.A_ID=AGREEMENT_ID AND SRC_CD=AGREEMENT_SOURCE_CD AND K_PF=HLDG_KEY_PFX AND K_SF=HLDG_KEY_SFX) as subquery' ,
            'partitionColumn': 'AGREEMENT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10,
            'table_pk':'AGREEMENT_ID, AGREEMENT_SOURCE_CD ,HLDG_KEY_PFX ,HLDG_KEY ,HLDG_KEY_SFX' 

        },
        'BP_CREDENTIAL_VW': {
            'sql_query': '(SELECT * FROM BP_CREDENTIAL_VW WHERE TRANS_DT BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date)) as subquery' ,
            'partitionColumn': 'PRTY_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10 ,
            'table_pk':'BP_id, prty_id, cred_desc, cred_st_dt'

        },
        'CUST_AGMT_CMN_VW': {
            'sql_query': '(select CMN.*,A_ID,H_PRTY_ID from CUST_AGMT_CMN_VW CMN RIGHT OUTER JOIN (SELECT AGREEMENT_ID A_ID,PRTY_ID H_PRTY_ID FROM CUST_AGMT_HIST_VW WHERE CAST(CUST_AGMT_HIST_FR_DT AS DATE) BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) OR CAST(CUST_AGMT_HIST_TO_DT AS DATE) BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) GROUP BY 1,2)HIST ON A_ID=CMN.AGREEMENT_ID AND H_PRTY_ID=CMN.PRTY_ID) as subquery',
            'partitionColumn': 'A_ID MOD 50',
            'lowerBound': 0,
            'upperBound': 49,
            'numPartitions': 50,
            'table_pk':'AGREEMENT_ID, prty_id'                     
        },
        'CUST_DEMOGRAPHICS_VW': {
            'sql_query': '(SELECT CMN.*,hist_prty_id FROM CUST_DEMOGRAPHICS_VW cmn right outer JOIN (SELECT PRTY_ID hist_prty_id from CUST_DEMOGRAPHICS_HIST_VW WHERE CAST(CUST_HIST_FR_DT AS DATE) BETWEEN cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date) or CAST(CUST_HIST_TO_DT AS DATE) BETWEEN cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date)   group by 1) hist on cmn.prty_id=hist_prty_id) as subquery',
            'partitionColumn': 'PRTY_ID MOD 50',
            'lowerBound': 0,
            'upperBound': 49,
            'numPartitions': 50,
            'table_pk':' prty_id'                      
        },
        'FUND_CMN_VW': {
            'sql_query': '(SELECT * FROM FUND_CMN_VW) as subquery'                     
        },
        'PDCR_AGMT_CMN_VW': {
            'sql_query': '(select cmn.*,hist_agreement_id,hist_prty_id from PDCR_AGMT_CMN_VW CMN right outer join (select agreement_id hist_agreement_id,prty_id hist_prty_id from pdcr_agmt_hist_vw where PDCR_AGMT_HIST_FR_DT BETWEEN cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date) or PDCR_AGMT_HIST_to_DT BETWEEN cast('''$strt_dt''' as date) -5  AND cast('''$end_dt''' as date) group by 1,2 )hist on hist_agreement_id=cmn.agreement_id and hist_prty_id=cmn.prty_id ) as subquery',
            'partitionColumn': 'PRTY_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10 ,
            'table_pk':'AGREEMENT_ID, prty_id'                       
        },
        'PDCR_DEMOGRAPHICS_VW': {
            'sql_query': '(SELECT * FROM PDCR_DEMOGRAPHICS_VW) as subquery'

        },
        'SLLNG_AGMT_CMN_VW': {
            'sql_query': '(SELECT * FROM SLLNG_AGMT_CMN_VW WHERE cast(TRANS_DT as date) BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) ) as subquery',
            'table_pk':'CONTR_ID'   
        },
        'PRTY_ALT_ID_VW': {
            'sql_query': '(SELECT * FROM PROD_STND_PRTY_VW.PRTY_ALT_ID_VW WHERE cast(TRANS_DT as date) BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) or PRTY_ALT_ID_FR_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) or PRTY_ALT_ID_TO_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date)   ) as subquery',
            'partitionColumn': 'PRTY_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10,
            'table_pk':'PRTY_ID,ALT_ID,PRTY_ALT_ID_FR_DT'   
        },
        'AGMT_PAC_DATA_VW': {
            'sql_query': '(select * from PROD_PAC_VW.AGMT_PAC_DATA_VW where AGMT_PAC_DATA_TO_DT between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date)  or AGMT_PAC_DATA_FR_DT  between cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date)  or TRANS_DT BETWEEN cast('''$strt_dt''' as date) -5 AND cast('''$end_dt''' as date) ) as subquery',
            'partitionColumn': 'AGMT_ID MOD 10',
            'lowerBound': 0,
            'upperBound': 9,
            'numPartitions': 10,
            'table_pk':'AGMT_ID, agmt_pac_data_fr_dt'   
        }
    }
}

"

