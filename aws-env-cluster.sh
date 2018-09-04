# VERTICA SETTINGS
export VERTICA_CONNECTION_ID="vertica_aws_nprd"
export VERTICA_HOST="vertica.dsawsnprd.massmutual.com"
export VERTICA_USER="dbadmin"
export VERTICA_DATABASE="advana"
export VERTICA_VSQL="/usr/local/bin/vsql"

# HDFS SETTINGS
export HADOOP_CONF_DIR=/etc/hadoop/conf

# JARS
export PY4JDBC_JAR="/hadoop/deploy/jdbc/py4jdbc-assembly-0.1.6.8_211.jar"
export JARS="py4jdbc_latest.jar" # comma separated

# SQUARK SETTINGS
export SQUARK_PASSWORD_FILE=$(pwd)/.squark_password
export SQUARK_CONFIG_DIR=$(pwd)/config
export PYTHON_VENV=$(pwd)/virt
export WAREHOUSE_DIR="/_wh_dev/"
export SQUARK_DELETED_TABLE_SUFFIX="_ADVANA_DELETED"

# SPARK SETTINGS
export SPARK_HOME=/hadoop/spark/2.2
export PYSPARK_PYTHON=$PYTHON_VENV/bin/python
export SPARK_DRIVER_MEMORY="2G"
export SPARK_EXECUTOR_MEMORY="2G"
export CLASSPATH=$CLASSPATH:$PY4JDBC_JAR
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$(pwd)

# for AWS EMR
# instead of default "jenkins" in run.sh
export SPARK_YARN_QUEUE="default"

export USE_CLUSTER_EMR=1