truncate table prod_stnd_prty.PRTY_ALT_ID ;

INSERT /*+ direct */
INTO
    prod_stnd_prty.PRTY_ALT_ID
    (
        PRTY_ID,
        ALT_ID_TYP_CD,
        ALT_ID_STYP_CD,
        PRTY_ALT_ID_FR_DT,
        PRTY_ALT_ID_TO_DT,
        SRC_ALT_ID_TYP_CD,
        SRC_ALT_ID_STYP_CD,
        ALT_ID,
        BUS_STRT_DT,
        BUS_END_DT,
        STUS_CD,
        SRC_STUS_CD,
        ISSUE_AUTH,
        SRC_SYS_PRTY_ID,
        PRTY_DATA_FR_DT,
        CURR_IND,
        SRC_DEL_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    select PRTY_ID,
        ALT_ID_TYP_CD,
        ALT_ID_STYP_CD,
        PRTY_ALT_ID_FR_DT,
        PRTY_ALT_ID_TO_DT,
        SRC_ALT_ID_TYP_CD,
        SRC_ALT_ID_STYP_CD,
        ALT_ID,
        BUS_STRT_DT,
        BUS_END_DT,
        STUS_CD,
        SRC_STUS_CD,
        ISSUE_AUTH,
        SRC_SYS_PRTY_ID,
        PRTY_DATA_FR_DT,
        CURR_IND,
        SRC_DEL_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT from squark_staging.prty_alt_id;

commit ;
