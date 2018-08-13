export PROJECT_ID=haven_cdmproducers
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='"%s"'
export CHECK_PRIVS=1
# cdmproducers db
export CONNECTION_ID=haven_cdm
export SQUARK_METADATA=1


export JSON_INFO="
{
	'SAVE_TABLE_SQL_SUBQUERY':{
      'schema': 'dbo',
      'table_queries': {
            'commissionable_event_info': '(SELECT \\\"id\\\",\\\"externalId\\\",\\\"eventType\\\",\\\"transactionDate\\\",\\\"producerId\\\",\\\"invoiceId\\\",\\\"originalExternalId\\\",\\\"productCode\\\",\\\"policyFaceAmount\\\",\\\"policyTerm\\\",\\\"value\\\",\\\"insuredLastName\\\",\\\"policyIssuedState\\\",\\\"securedDate\\\",\\\"policyIssuedDate\\\",\\\"policyEndDate\\\",\\\"terminationDate\\\",\\\"billingCycleDate\\\",\\\"policyEffectiveDate\\\",CAST(\\\"flatExtraAmount\\\" AS VARCHAR(65000)) AS flatExtraAmount,CAST(\\\"waiverPremiumAmount\\\" AS VARCHAR(65000)) AS waiverPremiumAmount,\\\"createdAt\\\",\\\"updatedAt\\\",\\\"commissionableEventId\\\" FROM commissionable_event_info) as subquery'
        }
    }
}"


