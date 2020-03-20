truncate table prod_stnd_crcog_tbls.RCOG_QUAL_YR ;

INSERT
INTO
    prod_stnd_crcog_tbls.RCOG_QUAL_YR
    (
        BP_ID,
        RECOG_QUAL_AS_OF_DT,
        RCOG_AWD_ID,
        TOT_YR_QUAL_CNT,
        LAST_QUAL_YR,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        SRC_DEL_IND,
        CURR_IND
    )
    select BP_ID,
        RECOG_QUAL_AS_OF_DT,
        RCOG_AWD_ID,
        TOT_YR_QUAL_CNT,
        LAST_QUAL_YR,
        SRC_SYS_ID,
        RUN_ID,
        UPDT_RUN_ID,
        TRANS_DT,
        SRC_DEL_IND,
        CURR_IND from squark_staging.RCOG_QUAL_YR_VW
    ;

commit ;

