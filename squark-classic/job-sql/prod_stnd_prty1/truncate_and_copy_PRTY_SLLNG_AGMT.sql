truncate table prod_stnd_prty.PRTY_SLLNG_AGMT ;

INSERT /*+ direct */
INTO
    prod_stnd_prty.PRTY_SLLNG_AGMT
    (
        CONTR_ID,
        PRTY_ID,
        PRTY_SLLING_AGMT_FR_DT,
        PRTY_SLLING_AGMT_TO_DT,
        PRTY_DATA_FR_DT,
        CONTR_SRC_ALT_ID,
        PRTY_SRC_ALT_ID,
        SLLNG_AGMT_RLE_CD,
        SLLNG_AGMT_REL_TYP_CD,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    select CONTR_ID,
        PRTY_ID,
        PRTY_SLLING_AGMT_FR_DT,
        PRTY_SLLING_AGMT_TO_DT,
        PRTY_DATA_FR_DT,
        CONTR_SRC_ALT_ID,
        PRTY_SRC_ALT_ID,
        SLLNG_AGMT_RLE_CD,
        SLLNG_AGMT_REL_TYP_CD,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT from squark_staging.PRTY_SLLNG_AGMT ;

commit ;
