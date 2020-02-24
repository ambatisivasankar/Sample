--- delete data for this date batch from the target table
--- TRANS_DT is the date column used for INCREMENTAL batches
DELETE FROM
    teradata_finhist_prd.AGMT_FIN_TXN_CMN_VW
WHERE
    TRANS_DT IN (
        SELECT
            TRANS_DT
        FROM
            squark_staging.AGMT_FIN_TXN_CMN_VW_prd
        GROUP BY 1
    )
;

--- Insert all the data from staging into target
INSERT INTO teradata_finhist_prd.AGMT_FIN_TXN_CMN_VW
SELECT
    *
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW_prd
;
COMMIT;
