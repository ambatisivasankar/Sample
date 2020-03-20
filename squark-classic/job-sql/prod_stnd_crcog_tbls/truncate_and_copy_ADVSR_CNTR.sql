truncate table prod_stnd_crcog_tbls.ADVSR_CNTR ;

INSERT
INTO
    prod_stnd_crcog_tbls.ADVSR_CNTR
    (
        PDCR_BPID,
        DST_BPID,
        DST_REL_STRT_DT,
        DST_REL_END_DT,
        DST_BUS_STRT_DT,
        DST_BUS_END_DT,
        CNTR_B_ID,
        DST_LEVEL_PARSED,
        SLLNG_AGMT_DTL_FR_DT,
        PDCR_SLLNG_AGMT_RLE_CD,
        L0_PARENT_BPID,
        L0_PRNT_SLLNG_AGMT_RLE,
        L0_REL_STRT_DT,
        L0_REL_END_DT,
        L0_BUS_STRT_DT,
        L0_BUS_END_DT,
        L0_STD_CONTR_TYP_DESC,
        L0_STD_CONTR_TYP_CD,
        L1_PARENT_BPID,
        L1_PRNT_SLLNG_AGMT_RLE,
        L1_REL_STRT_DT,
        L1_REL_END_DT,
        L1_BUS_STRT_DT,
        L1_BUS_END_DT,
        L1_STD_CONTR_TYP_DESC,
        L1_STD_CONTR_TYP_CD,
        L2_PARENT_BPID,
        L2_PRNT_SLLNG_AGMT_RLE,
        L2_REL_STRT_DT,
        L2_REL_END_DT,
        L2_BUS_STRT_DT,
        L2_BUS_END_DT,
        L2_STD_CONTR_TYP_DESC,
        L2_STD_CONTR_TYP_CD,
        L3_PARENT_BPID,
        L3_PRNT_SLLNG_AGMT_RLE,
        L3_REL_STRT_DT,
        L3_REL_END_DT,
        L3_BUS_STRT_DT,
        L3_BUS_END_DT,
        L3_STD_CONTR_TYP_DESC,
        L3_STD_CONTR_TYP_CD,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT
    )
    select 
      PDCR_BPID,
        DST_BPID,
        DST_REL_STRT_DT,
        DST_REL_END_DT,
        DST_BUS_STRT_DT,
        DST_BUS_END_DT,
        CNTR_B_ID,
        DST_LEVEL_PARSED,
        SLLNG_AGMT_DTL_FR_DT,
        PDCR_SLLNG_AGMT_RLE_CD,
        L0_PARENT_BPID,
        L0_PRNT_SLLNG_AGMT_RLE,
        L0_REL_STRT_DT,
        L0_REL_END_DT,
        L0_BUS_STRT_DT,
        L0_BUS_END_DT,
        L0_STD_CONTR_TYP_DESC,
        L0_STD_CONTR_TYP_CD,
        L1_PARENT_BPID,
        L1_PRNT_SLLNG_AGMT_RLE,
        L1_REL_STRT_DT,
        L1_REL_END_DT,
        L1_BUS_STRT_DT,
        L1_BUS_END_DT,
        L1_STD_CONTR_TYP_DESC,
        L1_STD_CONTR_TYP_CD,
        L2_PARENT_BPID,
        L2_PRNT_SLLNG_AGMT_RLE,
        L2_REL_STRT_DT,
        L2_REL_END_DT,
        L2_BUS_STRT_DT,
        L2_BUS_END_DT,
        L2_STD_CONTR_TYP_DESC,
        L2_STD_CONTR_TYP_CD,
        L3_PARENT_BPID,
        L3_PRNT_SLLNG_AGMT_RLE,
        L3_REL_STRT_DT,
        L3_REL_END_DT,
        L3_BUS_STRT_DT,
        L3_BUS_END_DT,
        L3_STD_CONTR_TYP_DESC,
        L3_STD_CONTR_TYP_CD,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT from squark_staging.ADVSR_CNTR_VW ;
    ;


commit ;
