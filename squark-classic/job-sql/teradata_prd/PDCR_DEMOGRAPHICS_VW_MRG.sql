TRUNCATE TABLE DMD_TEST.PDCR_DEMOGRAPHICS_VW;
select copy_table('SQUARK_STAGING.PDCR_DEMOGRAPHICS_VW','DMD_TEST.PDCR_DEMOGRAPHICS_VW');
COMMIT;