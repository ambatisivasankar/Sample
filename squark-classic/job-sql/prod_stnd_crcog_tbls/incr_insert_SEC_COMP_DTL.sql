       
INSERT
INTO
    prod_stnd_crcog_tbls.SEC_COMP_DTL
    (
        COMM_CLOS_DT,
        SRC_CORG_IDENT,
        TRLGY_LEDG_ID,
        SRC_TXN_ID,
        SEC_COMP_TYP_CD,
        SRC_SEC_COMP_TYP_CD,
        INHRT_RCRT_CD,
        SRC_INHRT_RCRT_CD,
        SEC_COMP_TXN_PAYABLE_AMT,
        REM_RATE,
        PAY_SYS_ACK_DT,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        CURR_IND
    )
    select VW.COMM_CLOS_DT,
        VW.SRC_CORG_IDENT,
        VW.TRLGY_LEDG_ID,
        VW.SRC_TXN_ID,
        VW.SEC_COMP_TYP_CD,
        VW.SRC_SEC_COMP_TYP_CD,
        VW.INHRT_RCRT_CD,
        VW.SRC_INHRT_RCRT_CD,
        VW.SEC_COMP_TXN_PAYABLE_AMT,
        VW.REM_RATE,
        VW.PAY_SYS_ACK_DT,
        VW.SRC_SYS_ID,
        VW.RUN_ID,
        VW.UPDT_RUN_ID,
        VW.TRANS_DT,
        VW.CURR_IND from squark_staging.SEC_COMP_DTL_VW VW left join prod_stnd_crcog_tbls.SEC_COMP_DTL F on F.TRLGY_LEDG_ID = VW.TRLGY_LEDG_ID and VW.src_corg_ident = F.src_corg_ident 
        and VW.PAY_SYS_ACK_DT = F.PAY_SYS_ACK_DT where F.TRLGY_LEDG_ID is null
    ;

    commit ;