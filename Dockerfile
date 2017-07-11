#FROM ubuntu:latest
FROM centos:7

# Install basic deps
RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm
RUN yum -y update && yum install -y libffi-devel libssl-devel wget gcc make python35u python35u-libs python35u-devel python35u-pip java-1.8.0-openjdk.x86_64 openssl-devel.x86_64 openssl-libs.x86_64

# Install java and sbt
RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo
RUN yum install -y sbt # 0.13.15-2 <- If latest fails go back to this version
 
# Install spark.
RUN curl -L -o spark.tgz http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.6.tgz \
    && mkdir -p spark \
    && tar -xf spark.tgz -C /spark --strip-components=1 \
    && rm spark.tgz \ 
    && chown root:root -R /spark

# Install vertica jdbc jar.
RUN curl -o /usr/local/lib/vertica-jdbc-7.2.3-0.jar https://my.vertica.com/client_drivers/7.2.x/7.2.3-0/vertica-jdbc-7.2.3-0.jar

# Install python deps.
ADD squark/requirements.txt /app/requirements.txt
RUN pip3.5 install --upgrade pip \
    && pip3.5 install -r /app/requirements.txt \
    && rm -rf ~/.cache/pip

# Add extra software
RUN yum -y install zip
# FOR TESTING
#RUN yum -y install vim
#RUN yum -y install iputils-ping

ENV CLASSPATH=/usr/local/share/py4jdbc/py4jdbc-assembly-latest.jar:/usr/local/share/py4jdbc/py4jdbc-assembly-0.1.6.8.jar:/usr/local/lib/vertica-jdbc-7.2.3-0.jar:/usr/local/lib/postgresql-9.4.1211.jre6.jar
ENV SPARK_HOME=/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYTHONPATH="$PYTHONPATH:$SPARK_HOME/python:/usr/local/src/squark-advana"
ENV SQUARK_PASSWORD_FILE="/usr/local/src/squark-advana/.squark-password"
ENV SQUARK_CONFIG_DIR="/usr/local/src/squark-advana/config"
ENV PYSPARK_PYTHON="/usr/local/src/squark-advana/virt/bin/python3"
ENV HADOOP_CONF_DIR="/etc/hadoop/conf/"
ENV VERTICA_CONNECTION_ID="test_vertica"
ENV PY4JDBC_JAR="/usr/local/share/py4jdbc/py4jdbc-assembly-0.1.6.8.jar"
ENV SQUARK_PYTHON="/usr/local/src/squark-advana/virt/bin/python3"
ENV HDFS_HOST="hdfs"
ENV HDFS_PORT="50070"
ENV HDFS_USER="hdfs"
ENV SPARK_DRIVER_MEMORY="1G"
ENV SPARK_EXECUTOR_MEMORY="1G"
ENV JARS="/usr/local/lib/postgresql-9.4.1211.jre6.jar"
ENV VERTICA_VSQL="tarballs/opt/vertica/bin/vsql"
ENV PYTHON_VENV="/usr/local/src/squark-advana/virt"
ENV VERTICA_HOST="vertica"
ENV VERTICA_USER="dbadmin"
ENV VERTICA_PASSWORD=""
#ENV SQUARK_PYTHON="/usr/local/src/squark-advana/virt/bin/python3"
WORKDIR /usr/local/src/squark-advana
ADD ./squark-classic/hadoop_confs/etc/hadoop /etc/hadoop
ADD . /usr/local/src/squark-advana

RUN rm /usr/bin/python
RUN ln -s /usr/bin/python2.7 /usr/bin/python
RUN ln -s /usr/bin/python3.5 /usr/bin/python3

RUN /usr/local/src/squark-advana/bootstrap.sh

RUN cp /usr/local/src/squark-advana/.jars/postgresql-9.4.1211.jre6.jar /usr/local/lib/postgresql-9.4.1211.jre6.jar
RUN mkdir /usr/local/share/py4jdbc
RUN cp /usr/local/src/squark-advana/.jars/py4jdbc-assembly-latest.jar /usr/local/share/py4jdbc/py4jdbc-assembly-latest.jar
RUN cp /usr/local/src/squark-advana/.jars/py4jdbc-assembly-latest.jar /usr/local/share/py4jdbc/py4jdbc-assembly-0.1.6.8.jar

#ADD docker-entrypoint.sh / 
#ENTRYPOINT ["/docker-entrypoint.sh"]
