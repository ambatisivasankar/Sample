truncate table prod_stnd_crcog_tbls.UNIT ;

INSERT
INTO
    prod_stnd_crcog_tbls.UNIT
    (
        UNIT_ID,
        AGY_BPID,
        AGY_PRTY_ID,
        UNIT_BPID,
        UNIT_NBR,
        UNIT_NM,
        UNIT_FRM_DT,
        UNIT_TO_DT,
        UNIT_OPEN_DT,
        UNIT_CLOSE_DT,
        UNIT_ACTV_IND,
        UNIT_MGR_BPID,
        UNIT_MGR_PRTY_ID,
        UNIT_MGR_NM,
        UNIT_MGR_TTL,
        UNIT_MGR_TYP,
        UNIT_MGR_STRT_DT,
        UNIT_MGR_STP_DT,
        SM_SPLT_PCT,
        SM_SPLT_STRT_DT,
        SM_SPLT_END_DT,
        UNIT_MGR_ACTV_IND,
        MA_BPID,
        MA_PRTY_ID,
        MA_NM,
        MA_STRT_DT,
        MA_END_DT,
        MA_UNIT_ACTV_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        UNIT_TYP,
        AGY_NR,
        SRC_DEL_IND,
        ADVSR_BPID,
        ADVSR_MA_END_DT,
        ADVSR_MA_STRT_DT,
        ADVSR_NM,
        ADVSR_PRTY_ID,
        ADVSR_UNIT_ACTV_IND,
        CURR_IND,
        ADVSR_UNIT_STRT_DT,
        ADVSR_UNIT_STP_DT
    )
    select UNIT_ID,
        AGY_BPID,
        AGY_PRTY_ID,
        UNIT_BPID,
        UNIT_NBR,
        UNIT_NM,
        UNIT_FRM_DT,
        UNIT_TO_DT,
        UNIT_OPEN_DT,
        UNIT_CLOSE_DT,
        UNIT_ACTV_IND,
        UNIT_MGR_BPID,
        UNIT_MGR_PRTY_ID,
        UNIT_MGR_NM,
        UNIT_MGR_TTL,
        UNIT_MGR_TYP,
        UNIT_MGR_STRT_DT,
        UNIT_MGR_STP_DT,
        SM_SPLT_PCT,
        SM_SPLT_STRT_DT,
        SM_SPLT_END_DT,
        UNIT_MGR_ACTV_IND,
        MA_BPID,
        MA_PRTY_ID,
        MA_NM,
        MA_STRT_DT,
        MA_END_DT,
        MA_UNIT_ACTV_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        UNIT_TYP,
        AGY_NR,
        SRC_DEL_IND,
        ADVSR_BPID,
        ADVSR_MA_END_DT,
        ADVSR_MA_STRT_DT,
        ADVSR_NM,
        ADVSR_PRTY_ID,
        ADVSR_UNIT_ACTV_IND,
        CURR_IND,
        ADVSR_UNIT_STRT_DT,
        ADVSR_UNIT_STP_DT from squark_staging.UNIT_VW ;

commit ;
