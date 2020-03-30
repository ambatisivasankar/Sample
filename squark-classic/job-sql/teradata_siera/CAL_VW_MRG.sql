--- remove the records in staging that are already present in the actual table
DELETE FROM SQUARK_STAGING.CAL_VW WHERE (_advana_md5) in (select _advana_md5 from TERADATA_SIERA.CAL_VW);
COMMIT;

--- insert the remaining records
INSERT INTO TERADATA_SIERA.CAL_VW
SELECT * FROM SQUARK_STAGING.CAL_VW ;
COMMIT;
