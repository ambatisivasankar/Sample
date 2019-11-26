#!/usr/bin/env bash
###############################################################
## Run - This does a Spark-Submit

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o errexit
set -o nounset
set -o pipefail
set +x
set -e

TZ_NAME=UTC
export TZ=":${TZ_NAME}"

cd "${WORKSPACE}/squark-classic"
cp "${PY4JDBC_JAR}" py4jdbc_latest.jar
zip -r -q squark.zip .

"${SPARK_HOME}"/bin/spark-submit \
--master yarn \
--executor-cores "${SPARK_CORE_COUNT:-1}" \
--files tls-ca-bundle.jks \
--conf "spark.yarn.queue=${SPARK_YARN_QUEUE:-jenkins}" \
--conf "spark.dynamicAllocation.maxExecutors=${SPARK_MAX_EXECUTORS:-2}" \
--conf "spark.driver.extraJavaOptions=-Duser.timezone=${TZ_NAME}" \
--conf "spark.executor.extraJavaOptions=-Duser.timezone=${TZ_NAME}" \
--conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" \
--conf "spark.executor.extraClassPath=${JARS}" \
--conf "spark.executorEnv.TZ=${TZ}" \
--conf "spark.debug.maxToStringFields=100" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--conf "spark.hadoop.fs.s3a.enableServerSideEncryption=true" \
--conf "spark.hadoop.fs.s3a.serverSideEncryptionAlgorithm=AES256" \
--conf "spark.yarn.appMasterEnv.TZ=${TZ}" \
--conf "spark.sql.session.timeZone=${TZ_NAME}" \
--conf "spark.app.name=${PROJECT_ID}-squark-all-tables" \
--driver-memory "${SPARK_DRIVER_MEMORY:-1G}" \
--executor-memory "${SPARK_EXECUTOR_MEMORY:-1G}" \
--driver-class-path "${JARS}" \
--driver-library-path "${JARS}" \
--py-files squark.zip \
--jars "${JARS}" \
--packages "com.databricks:spark-csv_2.10:1.4.0,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2" \
all_tables.py
