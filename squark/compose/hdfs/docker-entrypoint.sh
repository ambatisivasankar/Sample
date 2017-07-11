#! /usr/bin/env bash

DAEMONS="\
    mysqld \
    cloudera-quickstart-init"

DAEMONS="${DAEMONS} \
    zookeeper-server \
    hadoop-hdfs-datanode \
    hadoop-hdfs-journalnode \
    hadoop-hdfs-namenode \
    hadoop-hdfs-secondarynamenode \
    hadoop-httpfs \
    hadoop-mapreduce-historyserver \
    hadoop-yarn-nodemanager \
    hadoop-yarn-resourcemanager"

for daemon in ${DAEMONS}; do
    sudo service ${daemon} start
done

tail \
  -f /var/log/hadoop-hdfs/hadoop-hdfs-datanode-quickstart.cloudera.out \
  -f /var/log/hadoop-hdfs/hadoop-hdfs-journalnode-quickstart.cloudera.out \
  -f /var/log/hadoop-hdfs/hadoop-hdfs-namenode-quickstart.cloudera.out \
  -f /var/log/hadoop-hdfs/hadoop-hdfs-secondarynamenode-quickstart.cloudera.out \
  -f /var/log/hadoop-mapreduce/mapred-mapred-historyserver-quickstart.cloudera.out \
  -f /var/log/hadoop-yarn/yarn-yarn-nodemanager-quickstart.cloudera.out \
  -f /var/log/hadoop-yarn/yarn-yarn-resourcemanager-quickstart.cloudera.out
#  -f /var/log/hbase/hbase-hbase-master-quickstart.cloudera.out \
#  -f /var/log/hbase/hbase-hbase-rest-quickstart.cloudera.out \
#  -f /var/log/hbase/hbase-hbase-thrift-quickstart.cloudera.out \
#  -f /var/log/hbase/hbase-hbase-regionserver-quickstart.cloudera.out

