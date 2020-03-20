truncate table prod_stnd_crcog_tbls.AGY_RENT_ADJ ;

	
INSERT
INTO
    prod_stnd_crcog_tbls.AGY_RENT_ADJ
    (
        AGY_BP_ID,
        AGY_NR,
        AGY_ADJ_RENT_AMT,
        AGY_RENT_YR,
        AGY_RENT_MO,
        CHG_TYP,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        SRC_DEL_IND,
        CURR_IND,
        AGY_LEASE_ID
    )
    select  AGY_BP_ID,
        AGY_NR,
        AGY_ADJ_RENT_AMT,
        AGY_RENT_YR,
        AGY_RENT_MO,
        CHG_TYP,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        SRC_DEL_IND,
        CURR_IND,
        AGY_LEASE_ID from squark_staging.AGY_RENT_ADJ_VW ;

commit ;
