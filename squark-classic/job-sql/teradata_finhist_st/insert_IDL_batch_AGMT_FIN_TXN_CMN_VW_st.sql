--- delete data for this date batch from the target table
--- TRANS_EFFECTIVE_DATE is the date column used for IDL batches
DELETE FROM
    teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
WHERE
    TRANS_EFFECTIVE_DATE IN (
        SELECT
            TRANS_EFFECTIVE_DATE
        FROM
            squark_staging.AGMT_FIN_TXN_CMN_VW_st
        GROUP BY 1
    )
;

--- Insert all the data from staging into target
INSERT INTO teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
SELECT
    *
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW_st
;
COMMIT;
