--- Merges the staging table and the target table

--- Remove records from staging that exist already in target
--- But only where the record exists in an unchanged state (hashes match)
DELETE FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW
WHERE
    (agreement_id, _advana_md5) IN (
        SELECT
            agreement_id, _advana_md5
        FROM
            teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
    )
;
COMMIT;

--- Remove records from target that exist in staging
--- These records have been changed (otherwise the previous block would have captured them)
DELETE FROM
    teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
WHERE
    (agreement_id) IN (
        SELECT
            agreement_id
        FROM
            squark_staging.AGMT_FIN_TXN_CMN_VW
    )
;
COMMIT;

--- Insert all the data from staging into target
INSERT INTO teradata_finhist_st.AGMT_FIN_TXN_CMN_VW
SELECT
    *
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW
;
COMMIT;
