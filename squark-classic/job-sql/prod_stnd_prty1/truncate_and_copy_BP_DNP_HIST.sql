truncate table prod_stnd_prty.BP_DNP_HIST ;

INSERT /*+ direct */
INTO
    prod_stnd_prty.BP_DNP_HIST
    (
        BP_DNP_HIST_SID,
        BP_ID,
        PRTY_ID,
        DNP_EFF_FR_DT,
        DNP_EFF_TO_DT,
        DNP_IND,
        CURR_IND,
        SRC_DEL_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    select BP_DNP_HIST_SID,
        BP_ID,
        PRTY_ID,
        DNP_EFF_FR_DT,
        DNP_EFF_TO_DT,
        DNP_IND,
        CURR_IND,
        SRC_DEL_IND,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT from squark_staging.BP_DNP_HIST ;

commit ;
