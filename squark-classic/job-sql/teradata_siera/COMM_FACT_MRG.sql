


DELETE FROM SQUARK_STAGING.COMM_FACT WHERE (COMM_TXN_ID,_advana_md5) in (select COMM_TXN_ID,_advana_md5 from TERADATA_SIERA.COMM_FACT);
COMMIT;

DELETE FROM TERADATA_SIERA.COMM_FACT WHERE (COMM_TXN_ID) in ( select COMM_TXN_ID from SQUARK_STAGING.COMM_FACT );

INSERT INTO TERADATA_SIERA.COMM_FACT
SELECT * FROM SQUARK_STAGING.COMM_FACT ;
COMMIT;
