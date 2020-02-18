truncate table prod_stnd_prty.PDCR_DEMOGRAPHICS_phv ;
select copy_table('squark_staging.PDCR_DEMOGRAPHICS_vw','prod_stnd_prty.PDCR_DEMOGRAPHICS_phv' ) ;

