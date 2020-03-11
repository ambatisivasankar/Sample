truncate table prod_usig_cmn.SRC_DATA_TRNSLT ;

INSERT
INTO
    prod_usig_cmn.SRC_DATA_TRNSLT
    (
        SRC_DATA_TRNSLT_IDENT,
        SRC_CDE,
        SRC_TBL_NM,
        SRC_FLD_NM,
        SRC_FLD_VAL,
        TRNSLT_FLD_VAL,
        TRNSLT_FLD_NM,
        VA_SHT_DESC,
        VA_LNG_DESC,
        NUM_REP,
        LST_MOD_TS,
        LST_UPDT_OPER_CDE
    )
   select SRC_DATA_TRNSLT_IDENT,
        SRC_CDE,
        SRC_TBL_NM,
        SRC_FLD_NM,
        SRC_FLD_VAL,
        TRNSLT_FLD_VAL,
        TRNSLT_FLD_NM,
        VA_SHT_DESC,
        VA_LNG_DESC,
        NUM_REP,
        LST_MOD_TS,
        LST_UPDT_OPER_CDE from squark_staging.SRC_DATA_TRNSLT_VW ;

commit ;
