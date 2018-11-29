export PROJECT_ID=winrisk_prod
export WAREHOUSE_DIR='/_wh/'
export SQL_TEMPLATE='%s'
export CONNECTION_ID=winrisk
# 2018.11.29, winrisk_prod pulls from Winrisk Prod server, limit pulls to Sunday mornings
export INCLUDE_TABLES="MIB_REQUEST,MIB_REQUEST_SERVICE"
