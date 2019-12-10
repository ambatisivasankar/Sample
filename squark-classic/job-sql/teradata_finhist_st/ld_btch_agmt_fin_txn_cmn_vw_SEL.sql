---Get the nextbatches date range
SELECT
    '|'||MAX(strt_dt) ||'|'|| MAX(end_dt)||'|'|| 'LAST'
FROM
    :schema_name.AGMT_FIN_TXN_CMN_VW
WHERE
    actv_ind='Y'
;
