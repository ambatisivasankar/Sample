
set -e

#py4jdbc_jar="/usr/local/share/py4jdbc/py4jdbc-assembly-0.1.6.8.jar"
#cp /usr/local/lib/postgresql-9.4.1211.jre6.jar postgres_latest.jar
#cp $py4jdbc_jar py4jdbc_latest.jar
cp $PY4JDBC_JAR py4jdbc_latest.jar

zip -r -q squark.zip .

$SPARK_HOME/bin/spark-submit \
--master yarn \
--executor-cores ${SPARK_CORE_COUNT:-1} \
--conf "spark.yarn.queue=${SPARK_YARN_QUEUE:-jenkins}" \
--conf "spark.dynamicAllocation.maxExecutors=${SPARK_MAX_EXECUTORS:-2}" \
--driver-memory ${SPARK_DRIVER_MEMORY:-1G} \
--executor-memory ${SPARK_EXECUTOR_MEMORY:-1G} \
--driver-class-path "${JARS}" \
--driver-library-path "${JARS}" \
--conf "spark.executor.extraClassPath=${JARS}" \
--py-files squark.zip \
--jars "${JARS}" \
--packages "com.databricks:spark-csv_2.10:1.4.0,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2" \
all_tables.py
