truncate table prod_stnd_prty.PDCR_DEMOGRAPHICS_phv ;
INSERT INTO prod_stnd_prty.PDCR_DEMOGRAPHICS_PHV (PRTY_ID, PRTY_DATA_FR_DT, PRTY_DATA_TO_DT, PRTY_TYP_CD, SRC_PRTY_TYP_CD, PFX_NM, FIRST_NM, MID_NM, LST_NM, SFX_NM, FULL_NM, BRTH_DT, GOVT_ID_TYP, 
SRC_GOVT_ID_TYP, GOVT_ID_NR, GOVT_ID_CLS_CD, GOVT_ID_STUS_CD, GNDR_CD, SRC_GNDR_CD, DTH_CD, SRC_DTH_CD, DTH_DT, BRTH_DT_FLG_CD, BRTH_PLC, MARRIED_STUS_CD, SRC_MARRIED_STUS_CD, 
EDUC_LVL_CD, SRC_EDUC_LVL_CD, SNS_PRTY_IND, BUSINESS_PARTNER_ID, LST_CHNG_BY, ELEC_AD_TXT, PREFTEL_TEL_NR_ID, PREFTEL_CTRY_CD_NR, PREFTEL_AREA_CD_NR, PREFTEL_XCH_NR, 
PREFTEL_DIAL_NR, PREFTEL_XTN_NR, PREFTEL_NR_10DIGIT, RESTEL_TEL_NR_ID, RESTEL_CTRY_CD_NR, RESTEL_AREA_CD_NR, RESTEL_XCH_NR, RESTEL_DIAL_NR, RESTEL_XTN_NR, RESTEL_NR_10DIGIT, 
PREFAX_TEL_NR_ID, PREFAX_TEL_CTRY_CD_NR, PREFAX_TEL_AREA_CD_NR, PREFAX_TEL_XCH_NR, PREFAX_TEL_DIAL_NR, PREFAX_TEL_NR_10DIGIT, STNDADDR_AD_L1_TXT, STNDADDR_AD_L2_TXT, 
STNDADDR_AD_L3_TXT, STNDADDR_AD_L4_TXT, STNDADDR_CITY, STNDADDR_STATE, STNDADDR_CTRY_CD, STNDADDR_ZIP_1_5_NR, STNDADDR_ZIP_6_9_NR, STNDADDR_ZIP_10_13_NR, STNDADDR_FULL_POST_CD, 
STNDADDR_ADDR_VAL_CD, STNDADDR_ADDR_VER_DT, STNDADDR_DMST_FRGN_CD, STNDADDR_TIME_ZN, STNDADDR_SRC_TIME_ZN, STNDADDR_SRC_STATE, STNDADDR_SRC_CTRY_CD, BUSSADDR_AD_L1_TXT, 
BUSSADDR_AD_L2_TXT, BUSSADDR_AD_L3_TXT, BUSSADDR_AD_L4_TXT, BUSSADDR_CITY, BUSSADDR_STATE, BUSSADDR_CTRY_CD, BUSSADDR_ZIP_1_5_NR, BUSSADDR_ZIP_6_9_NR, BUSSADDR_ZIP_10_13_NR, 
BUSSADDR_FULL_POST_CD, BUSSADDR_ADDR_VAL_CD, BUSSADDR_ADDR_VER_DT, BUSSADDR_DMST_FRGN_CD, BUSSADDR_TIME_ZN, BUSSADDR_SRC_TIME_ZN, BUSSADDR_SRC_STATE, BUSSADDR_SRC_CTRY_CD, 
GA_YR_SRVC, NON_SEL_BP, RET_ST_DT, CARR_RNW_IND, CARR_RNW_ST_DT, RET_PLN, SRC_RET_PLN, REC_DT, DIS_IN, SRC_DIS_IND, PRIOR_INEXP_EXP, FIN_IND, SRC_FIN_IND, DIST_OFF, DIST_OFF_AGCY, 
DIST_OFF_ST_DT, DIST_OFF_END_DT, PREV_IND, FT_ST_DT, NASD_CD, DOA_BLOCKED, DOA_PAY_BPID, DOA_QUAL, MET_LEAD_LVL, MET_CNTR_QUAL, NADA_INEXP, LEAD_LVL, LEAD_LVL_ST_DT, LEAD_LVL_END_DT, 
NADA_ST_DT, RIS_LEAD_IND, RIS_LEAD_ST_DT, RIS_LEAD_END_DT, HOME_AGY_BPID, HOME_AGY_ID, HOME_AGY_TRNSF_DT, PRDC_REC_INHR, PROF_DSGN, PRTY_STUS, PRTY_STUS_RSN_CD, CAS_IND, BCC_START_DT,
 BCC_END_DT, LDRCONF_START_DT, LDRCONF_END_DT, DOING_BUSS_AS_NM, NICK_NM, LGCY_DSTR_ID, DTCHD_OFC_NM) 
 select PRTY_ID, PRTY_DATA_FR_DT, PRTY_DATA_TO_DT, PRTY_TYP_CD, SRC_PRTY_TYP_CD, PFX_NM, FIRST_NM, MID_NM, LST_NM, SFX_NM, FULL_NM, BRTH_DT, GOVT_ID_TYP, 
SRC_GOVT_ID_TYP, GOVT_ID_NR, GOVT_ID_CLS_CD, GOVT_ID_STUS_CD, GNDR_CD, SRC_GNDR_CD, DTH_CD, SRC_DTH_CD, DTH_DT, BRTH_DT_FLG_CD, BRTH_PLC, MARRIED_STUS_CD, SRC_MARRIED_STUS_CD, 
EDUC_LVL_CD, SRC_EDUC_LVL_CD, SNS_PRTY_IND, BUSINESS_PARTNER_ID, LST_CHNG_BY, ELEC_AD_TXT, PREFTEL_TEL_NR_ID, PREFTEL_CTRY_CD_NR, PREFTEL_AREA_CD_NR, PREFTEL_XCH_NR, 
PREFTEL_DIAL_NR, PREFTEL_XTN_NR, PREFTEL_NR_10DIGIT, RESTEL_TEL_NR_ID, RESTEL_CTRY_CD_NR, RESTEL_AREA_CD_NR, RESTEL_XCH_NR, RESTEL_DIAL_NR, RESTEL_XTN_NR, RESTEL_NR_10DIGIT, 
PREFAX_TEL_NR_ID, PREFAX_TEL_CTRY_CD_NR, PREFAX_TEL_AREA_CD_NR, PREFAX_TEL_XCH_NR, PREFAX_TEL_DIAL_NR, PREFAX_TEL_NR_10DIGIT, STNDADDR_AD_L1_TXT, STNDADDR_AD_L2_TXT, 
STNDADDR_AD_L3_TXT, STNDADDR_AD_L4_TXT, STNDADDR_CITY, STNDADDR_STATE, STNDADDR_CTRY_CD, STNDADDR_ZIP_1_5_NR, STNDADDR_ZIP_6_9_NR, STNDADDR_ZIP_10_13_NR, STNDADDR_FULL_POST_CD, 
STNDADDR_ADDR_VAL_CD, STNDADDR_ADDR_VER_DT, STNDADDR_DMST_FRGN_CD, STNDADDR_TIME_ZN, STNDADDR_SRC_TIME_ZN, STNDADDR_SRC_STATE, STNDADDR_SRC_CTRY_CD, BUSSADDR_AD_L1_TXT, 
BUSSADDR_AD_L2_TXT, BUSSADDR_AD_L3_TXT, BUSSADDR_AD_L4_TXT, BUSSADDR_CITY, BUSSADDR_STATE, BUSSADDR_CTRY_CD, BUSSADDR_ZIP_1_5_NR, BUSSADDR_ZIP_6_9_NR, BUSSADDR_ZIP_10_13_NR, 
BUSSADDR_FULL_POST_CD, BUSSADDR_ADDR_VAL_CD, BUSSADDR_ADDR_VER_DT, BUSSADDR_DMST_FRGN_CD, BUSSADDR_TIME_ZN, BUSSADDR_SRC_TIME_ZN, BUSSADDR_SRC_STATE, BUSSADDR_SRC_CTRY_CD, 
GA_YR_SRVC, NON_SEL_BP, RET_ST_DT, CARR_RNW_IND, CARR_RNW_ST_DT, RET_PLN, SRC_RET_PLN, REC_DT, DIS_IN, SRC_DIS_IND, PRIOR_INEXP_EXP, FIN_IND, SRC_FIN_IND, DIST_OFF, DIST_OFF_AGCY, 
DIST_OFF_ST_DT, DIST_OFF_END_DT, PREV_IND, FT_ST_DT, NASD_CD, DOA_BLOCKED, DOA_PAY_BPID, DOA_QUAL, MET_LEAD_LVL, MET_CNTR_QUAL, NADA_INEXP, LEAD_LVL, LEAD_LVL_ST_DT, LEAD_LVL_END_DT, 
NADA_ST_DT, RIS_LEAD_IND, RIS_LEAD_ST_DT, RIS_LEAD_END_DT, HOME_AGY_BPID, HOME_AGY_ID, HOME_AGY_TRNSF_DT, PRDC_REC_INHR, PROF_DSGN, PRTY_STUS, PRTY_STUS_RSN_CD, CAS_IND, BCC_START_DT,
 BCC_END_DT, LDRCONF_START_DT, LDRCONF_END_DT, DOING_BUSS_AS_NM, NICK_NM, LGCY_DSTR_ID, DTCHD_OFC_NM from squark_staging.PDCR_DEMOGRAPHICS_vw  ;

commit ;
