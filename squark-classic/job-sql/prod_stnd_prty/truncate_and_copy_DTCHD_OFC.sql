truncate table prod_stnd_prty.DTCHD_OFC ;

INSERT /*+ direct */
INTO
    prod_stnd_prty.DTCHD_OFC
    (
        AGY_PRTY_ID,
        AGY_BPID,
        AGY_NBR,
        ADVSR_BP_ID,
        ADVSR_PRTY_ID,
        ADVSR_DTCOFC_FR_DT,
        ADVSR_DTCOFC_TO_DT,
        DTCHD_OFC_ID,
        DTCHD_OFC_NM,
        DTCHD_OFC_FR_DT,
        DTCHD_OFC_TO_DT,
        DTCHD_OFC_SID,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        SRC_DEL_IND,
        CURR_IND
    )
    select AGY_PRTY_ID,
        AGY_BPID,
        AGY_NBR,
        ADVSR_BP_ID,
        ADVSR_PRTY_ID,
        ADVSR_DTCOFC_FR_DT,
        ADVSR_DTCOFC_TO_DT,
        DTCHD_OFC_ID,
        DTCHD_OFC_NM,
        DTCHD_OFC_FR_DT,
        DTCHD_OFC_TO_DT,
        DTCHD_OFC_SID,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        SRC_DEL_IND,
        CURR_IND from squark_staging.DTCHD_OFC
;

commit ;
