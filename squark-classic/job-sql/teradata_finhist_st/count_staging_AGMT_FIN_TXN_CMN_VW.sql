--- Count the records, used for debugging
SELECT
    COUNT(1)
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW
;


TRUNCATE TABLE squark_staging.AGMT_FIN_TXN_CMN_VW;
