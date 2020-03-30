--- remove the records in staging that are already present in the actual table
DELETE FROM SQUARK_STAGING.teradata_siera_sls_rptg_prem_hist_vw WHERE (_advana_md5) in (select _advana_md5 from TERADATA_SIERA.SLS_RPTG_PREM_HIST_VW);
COMMIT;

--- insert the remaining records
INSERT INTO TERADATA_SIERA.SLS_RPTG_PREM_HIST_VW
SELECT * FROM SQUARK_STAGING.teradata_siera_sls_rptg_prem_hist_vw ;
COMMIT;
