--- Replace the contents of actual with that of staging
TRUNCATE TABLE teradata_nb.DI_NB_RPT_VW;
SELECT COPY_TABLE('squark_staging.DI_NB_RPT_VW','teradata_nb.DI_NB_RPT_VW');
SELECT ANALYZE_STATISTICS('teradata_nb.DI_NB_RPT_VW');
