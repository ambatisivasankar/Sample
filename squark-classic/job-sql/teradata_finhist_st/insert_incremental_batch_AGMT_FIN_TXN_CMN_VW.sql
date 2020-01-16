--- Count the records, used for debugging
SELECT
    COUNT(1)
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW
;

--- delete data for this date batch from the target table
--- TRANS_DT is the date column used for INCREMENTAL batches
DELETE FROM
    teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
WHERE
    TRANS_DT IN (
        SELECT
            TRANS_DT
        FROM
            squark_staging.AGMT_FIN_TXN_CMN_VW
        GROUP BY 1
    )
;

--- Insert all the data from staging into target
INSERT INTO teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
SELECT
    *
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW
;
COMMIT;
