---Change this batches indicator from Y to N
UPDATE
    squark_staging.WRK_LD_BTCH_AGMT_FIN_TXN_CMN_VW_prd
SET
    ACTV_IND = 'N'
WHERE
    STRT_DT = :strt_dt
    AND END_DT = :end_dt
    AND ACTV_IND = 'Y'
;

COMMIT
;
