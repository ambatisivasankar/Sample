truncate table prod_stnd_prty.SLLNG_AGMT ;

INSERT
INTO
    prod_stnd_prty.SLLNG_AGMT
    (
        CONTR_ID,
        SLLNG_AGMT_FR_DT,
        SLLNG_AGMT_TO_DT,
        CONTR_SRC_ALT_ID,
        BUS_STRT_DT,
        BUS_END_DT,
        SLLNG_AGMT_TYP_CD,
        SLLNG_AGMT_STUS_CD,
        SLLNG_AGMT_STUS_RSN_CD,
        REL_STRT_DT,
        REL_END_DT,
        STD_CONTR_TYP_CD,
        OFFC_ID,
        OFFC_TYP_CD,
        DSTR_CHNL_CD,
        AGNCY_LVL_CD,
        UNIT_NBR,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    select CONTR_ID,
        SLLNG_AGMT_FR_DT,
        SLLNG_AGMT_TO_DT,
        CONTR_SRC_ALT_ID,
        BUS_STRT_DT,
        BUS_END_DT,
        SLLNG_AGMT_TYP_CD,
        SLLNG_AGMT_STUS_CD,
        SLLNG_AGMT_STUS_RSN_CD,
        REL_STRT_DT,
        REL_END_DT,
        STD_CONTR_TYP_CD,
        OFFC_ID,
        OFFC_TYP_CD,
        DSTR_CHNL_CD,
        AGNCY_LVL_CD,
        UNIT_NBR,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT from squark_staging.SLLNG_AGMT ;

commit ;
