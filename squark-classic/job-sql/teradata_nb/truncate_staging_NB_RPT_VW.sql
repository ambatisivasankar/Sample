--- Truncate squark_staging.NB_RPT_VW
CREATE TABLE IF NOT EXISTS squark_staging.NB_RPT_VW LIKE teradata_nb.NB_RPT_VW INCLUDING PROJECTIONS include schema PRIVILEGES;
TRUNCATE TABLE
    squark_staging.NB_RPT_VW
;
