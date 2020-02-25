truncate table prod_stnd_prty.SLLNG_AGMT_SM_SPLITS ;

INSERT
INTO
    PROD_STND_PRTY.SLLNG_AGMT_SM_SPLITS
    (
        CONTR_ID,
        CONTR_VER_NBR,
        PRD_PRTY_ID,
        SLLNG_AGMT_SM_SPLIT_FR_DT,
        SLLNG_AGMT_SM_SPLIT_TO_DT,
        SM_SPLIT_PCT,
        BUS_STRT_DT,
        BUS_END_DT,
        CHNGD_BY,
        INVLD_VER_IND,
        INVLD_OBJ_IND,
        PRD_SRC_PRTY_ALT_ID,
        PRD_CONTR_SRC_ALT_ID,
        SM_PRTY_ID,
        SM_SRC_PRTY_ALT_ID,
        SM_CONTR_ID,
        SM_CONTR_SRC_ALT_ID,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    SELECT CONTR_ID,
        CONTR_VER_NBR,
        PRD_PRTY_ID,
        SLLNG_AGMT_SM_SPLIT_FR_DT,
        SLLNG_AGMT_SM_SPLIT_TO_DT,
        SM_SPLIT_PCT,
        BUS_STRT_DT,
        BUS_END_DT,
        CHNGD_BY,
        INVLD_VER_IND,
        INVLD_OBJ_IND,
        PRD_SRC_PRTY_ALT_ID,
        PRD_CONTR_SRC_ALT_ID,
        SM_PRTY_ID,
        SM_SRC_PRTY_ALT_ID,
        SM_CONTR_ID,
        SM_CONTR_SRC_ALT_ID,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT FROM SQUARK_STAGING.SLLNG_AGMT_SM_SPLITS ;

COMMIT ;
