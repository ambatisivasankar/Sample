---Get the next batches date range
SELECT
    '|' || MAX(strt_dt) || '|' || MAX(end_dt)|| '|' || 'LAST'
FROM
    squark_staging.AGMT_FIN_TXN_CMN_VW
WHERE
    actv_ind = 'Y'
;
