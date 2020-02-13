--- Truncate squark_staging.DI_NB_RPT_VW
CREATE TABLE IF NOT EXISTS squark_staging.DI_NB_RPT_VW LIKE teradata_nb.DI_NB_RPT_VW INCLUDING PROJECTIONS include schema PRIVILEGES;

TRUNCATE TABLE
    squark_staging.DI_NB_RPT_VW
;
