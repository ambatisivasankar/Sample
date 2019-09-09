UPDATE :schema_name.wrk_ld_btch_agmt_hist_vw
SET ACTV_IND='N'
WHERE
    STRT_DT =:strt_dt
and
    END_DT=:end_dt
and
    ACTV_IND='Y'
    ;


COMMIT;