export PROJECT_ID=teradata_prod_stnd_vw
# primary purpose of schema is to refresh select schemas on a daily basis
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export INCLUDE_VIEWS=1
export INCLUDE_TABLES='AGMT_BILL_MODAL_PREM_VW'
export CONNECTION_ID=teradata_prod_stnd_vw

# export SPARK_MAX_EXECUTORS=10

# export JSON_INFO="
# {
#     'PARTITION_INFO':{
#         'tables': {
#             'AGMT_BILL_MODAL_PREM_VW': {
#               'partitionColumn': 'AGMT_ID MOD 50',
#               'lowerBound': 0,
#               'upperBound': 50,
#               'numPartitions': 50
#             }
#         }
#    }
# }
# "
