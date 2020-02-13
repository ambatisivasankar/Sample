--- Replace the contents of actual with that of staging
TRUNCATE TABLE teradata_nb.NB_RPT_VW;
SELECT COPY_TABLE('squark_staging.NB_RPT_VW','teradata_nb.NB_RPT_VW');
SELECT ANALYZE_STATISTICS('teradata_nb.NB_RPT_VW');
