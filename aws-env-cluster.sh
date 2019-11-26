# VERTICA SETTINGS

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x

# VERTICA SETTINGS
export AWS_VERTICA_PORT=5433
export VERTICA_USER="dbadmin"
export VERTICA_DATABASE="advana"
export VERTICA_VSQL="/usr/local/bin/vsql"
export VERTICA_ROOTCRTPATH="${WORKSPACE}/.vsql/root.crt"
export VERTICA_TRUSTSTOREPATH="${WORKSPACE}/squark-classic/tls-ca-bundle.jks"

# HDFS SETTINGS
export HADOOP_CONF_DIR="/etc/hadoop/conf"

# JARS
export JAR_VERSION="0.1.7.1_211"
export PY4JDBC_JAR="${WORKSPACE}/squark-classic/py4jdbc-assembly-${JAR_VERSION}.jar"
export JARS="py4jdbc-assembly-${JAR_VERSION}.jar" # comma separated

# SQUARK SETTINGS
export SQUARK_PASSWORD_FILE="${WORKSPACE}/.squark_password"
export SQUARK_CONFIG_DIR="${WORKSPACE}/config"
export PYTHON_VENV="${WORKSPACE}/virt"
export WAREHOUSE_DIR="/_wh_dev/"
export SQUARK_DELETED_TABLE_SUFFIX="_ADVANA_DELETED"
export SQUARK_NUM_RETRY=${SQUARK_NUM_RETRY:-1}

# SPARK SETTINGS
export SPARK_HOME="/hadoop/spark/2.2"
export PYSPARK_PYTHON="${PYTHON_VENV}/bin/python"
export SPARK_DRIVER_MEMORY="2G"
export SPARK_EXECUTOR_MEMORY="2G"
export CLASSPATH="${CLASSPATH:-x}:${PY4JDBC_JAR}"
export PYTHONPATH="${PYTHONPATH:-x}:${SPARK_HOME}/python:${WORKSPACE}"
export SPARK_CORE_COUNT="1"
export SPARK_MAX_EXECUTORS="2"
# for AWS EMR
# instead of default "jenkins" in run.sh
export SPARK_YARN_QUEUE="default"

export USE_CLUSTER_EMR=1

# SQUARK ENVIRONMENT SETTINGS
export NONPROD_S3_CONNECTION_ID="s3_nprd"
export PROD_S3_CONNECTION_ID="s3_prd"

export DEV_SQUARK_TYPE="squark-dev"
export QA_SQUARK_TYPE="squark-qa"
export PROD_SQUARK_TYPE="squark-prod"

export SQUARK_NONPROD_BUCKET="nonprd-squark-dev"
export SQUARK_PROD_BUCKET="dsprd-squark-prd"

export DEV_VERTICA_CONNECTION_ID="vertica_aws_nprd_dev"
export QA_VERTICA_CONNECTION_ID="vertica_aws_nprd_qa"
export PROD_VERTICA_CONNECTION_ID="vertica_aws_prd_prod"

export DEV_AWS_VERTICA_HOST="vertica-edw-dev.dsawsnprd.massmutual.com"
export QA_AWS_VERTICA_HOST="vertica-edw-qa.dsawsnprd.massmutual.com"
export PROD_AWS_VERTICA_HOST="vertica-edw-prod.dsawsprd.massmutual.com"

export DEV_JENKINS_URL="https://jenkins-data-engineering-dev.dsawsnprd.massmutual.com/"
export QA_JENKINS_URL="https://jenkins-data-engineering-qa.dsawsnprd.massmutual.com/"
export PROD_JENKINS_URL="https://jenkins-data-engineering-prod.dsawsprd.massmutual.com"

# LINKS
export BOOTSTRAP_DIR="${WORKSPACE}/scripts/bootstrap_cluster"
export LAUNCH_SQUARK_DIR="${WORKSPACE}/scripts/launch_squark_job"
export ENV_SETTINGS_DIR="${WORKSPACE}/scripts/environment_settings"
