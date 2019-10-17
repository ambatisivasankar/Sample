# VERTICA SETTINGS
export VERTICA_CONNECTION_ID="vertica_dev"
export VERTICA_HOST="vertica-dev"
export VERTICA_USER="dbadmin"
export VERTICA_DATABASE="advana"
export VERTICA_VSQL="/usr/local/bin/vsql"
export VERTICA_ROOTCRTPATH=${WORKSPACE}/.vsql/root.crt
export VERTICA_TRUSTSTOREPATH="${WORKSPACE}/squark-classic/tls-ca-bundle.jks"

# HDFS SETTINGS
export HDFS_HOST="devlx187"
export HDFS_PORT="50070"
export HDFS_USER="jenkins"
export HADOOP_CONF_DIR=/etc/hadoop/conf

# JARS
export JAR_VERSION='0.1.7.0_211'
export PY4JDBC_JAR="${WORKSPACE}/squark-classic/py4jdbc-assembly-"${JAR_VERSION}".jar"
export JARS="${PY4JDBC_JAR}" # comma separated

# SQUARK SETTINGS
export SQUARK_PASSWORD_FILE=$(pwd)/.squark_password
export SQUARK_CONFIG_DIR=$(pwd)/config
export PYTHON_VENV=$(pwd)/virt
export WAREHOUSE_DIR="/_wh_dev/"

# SPARK SETTINGS
export SPARK_HOME=/hadoop/spark/1.6
export PYSPARK_PYTHON=$PYTHON_VENV/bin/python
export SPARK_DRIVER_MEMORY="2G"
export SPARK_EXECUTOR_MEMORY="2G"
export CLASSPATH=$CLASSPATH:$PY4JDBC_JAR
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$(pwd)

