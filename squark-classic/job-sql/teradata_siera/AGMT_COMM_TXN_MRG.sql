


DELETE FROM SQUARK_STAGING.AGMT_COMM_TXN WHERE (COMM_TXN_ID,_advana_md5) in (select COMM_TXN_ID,_advana_md5 from dmd_test.AGMT_COMM_TXN);
COMMIT;

DELETE FROM DMD_TEST.AGMT_COMM_TXN WHERE (COMM_TXN_ID) in ( select COMM_TXN_ID from SQUARK_STAGING.AGMT_COMM_TXN );

INSERT INTO DMD_TEST.AGMT_COMM_TXN
SELECT * FROM SQUARK_STAGING.AGMT_COMM_TXN ;
COMMIT;
