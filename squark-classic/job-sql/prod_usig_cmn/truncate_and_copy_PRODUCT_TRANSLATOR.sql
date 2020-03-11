truncate table prod_usig_cmn.PRODUCT_TRANSLATOR ;

INSERT
INTO
    prod_usig_cmn.PRODUCT_TRANSLATOR
    (
        KND_MIN_CDE,
        KND_MAX_CDE,
        BSIS_MIN_CDE,
        BSIS_MAX_CDE,
        RATE_MIN_CDE,
        RATE_MAX_CDE,
        ADMN_SYS_GRP_CDE,
        LOB_CDE,
        LOB_NME,
        MAJOR_PROD_CDE,
        MAJOR_PROD_NME,
        MINOR_PROD_CDE,
        MINOR_PROD_NME,
        PROD_TYP_CDE,
        PROD_TYP_NME,
        BASE_RDR_CDE,
        QUAL_CDE,
        SER_YR,
        TIER_NR,
        XCS_BSIC_CDE,
        IMPLT_DT,
        ADMN_SYS_CDE,
        OPN_CLOS_BLCK_DESC,
        PROFT_CENT_PRD_CDE,
        PROD_ID
    )
   select KND_MIN_CDE,
        KND_MAX_CDE,
        BSIS_MIN_CDE,
        BSIS_MAX_CDE,
        RATE_MIN_CDE,
        RATE_MAX_CDE,
        ADMN_SYS_GRP_CDE,
        LOB_CDE,
        LOB_NME,
        MAJOR_PROD_CDE,
        MAJOR_PROD_NME,
        MINOR_PROD_CDE,
        MINOR_PROD_NME,
        PROD_TYP_CDE,
        PROD_TYP_NME,
        BASE_RDR_CDE,
        QUAL_CDE,
        SER_YR,
        TIER_NR,
        XCS_BSIC_CDE,
        IMPLT_DT,
        ADMN_SYS_CDE,
        OPN_CLOS_BLCK_DESC,
        PROFT_CENT_PRD_CDE,
        PROD_ID from squark_staging.PRODUCT_TRANSLATOR_VW
    ;

COMMIT ;
