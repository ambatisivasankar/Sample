truncate table prod_stnd_prty.SLLNG_AGMT_DTL ;

INSERT /*+ direct */
INTO
    prod_stnd_prty.SLLNG_AGMT_DTL
    (
        CONTR_ID,
        CONTR_VER_NBR,
        CONTR_SRC_ALT_ID,
        SLLNG_AGMT_TYP_CD,
        STD_CONTR_TYP_CD,
        BUS_STRT_DT,
        BUS_END_DT,
        SLLNG_AGMT_DTL_FR_DT,
        SLLNG_AGMT_DTL_TO_DT,
        CONTR_CRNCY_CD,
        SLLNG_AGMT_CHNG_RSN_CD,
        CHNGD_BY,
        INVLD_VER_IND,
        INVLD_OBJ_IND,
        PRMRY_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    select CONTR_ID,
        CONTR_VER_NBR,
        CONTR_SRC_ALT_ID,
        SLLNG_AGMT_TYP_CD,
        STD_CONTR_TYP_CD,
        BUS_STRT_DT,
        BUS_END_DT,
        SLLNG_AGMT_DTL_FR_DT,
        SLLNG_AGMT_DTL_TO_DT,
        CONTR_CRNCY_CD,
        SLLNG_AGMT_CHNG_RSN_CD,
        CHNGD_BY,
        INVLD_VER_IND,
        INVLD_OBJ_IND,
        PRMRY_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT from squark_staging.SLLNG_AGMT_DTL ;

commit ;
