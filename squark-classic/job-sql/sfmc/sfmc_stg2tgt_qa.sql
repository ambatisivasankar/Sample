

TRUNCATE TABLE sfmc.SF_MM_ACCOUNT;

INSERT INTO sfmc.SF_MM_ACCOUNT
SELECT * FROM squark_staging.FAUX_CVG_ID_HIST_VW;
COMMIT;

