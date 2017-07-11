FROM ubuntu:16.04

# Install gosu
ENV GOSU_VERSION 1.9
RUN set -x \
    && apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* \
    && dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')" \
    && wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch" \
    && wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc" \
    && export GNUPGHOME="$(mktemp -d)" \
    && gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
    && gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
    && rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc \
    && chmod +x /usr/local/bin/gosu \
    && gosu nobody true

# Install basic deps
RUN apt-get update && apt-get install -yqq python3.5 python3.5-dev python3-pip libffi-dev libssl-dev

# Install java and sbt
RUN apt-get update \
    && apt-get install -yqq apt-transport-https curl python2.7 \
    && ln -s /usr/bin/python2.7.distrib /usr/bin/python \
    && echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823 \
    && apt-get update && apt-get -yqq install default-jre sbt
 
# Install spark.
RUN curl -L -o spark.tgz http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.6.tgz \
    && mkdir -p spark \
    && tar -xf spark.tgz -C /spark --strip-components=1 \
    && rm spark.tgz \ 
    && chown root:root -R /spark

# Install vertica jdbc jar.
RUN curl -o /usr/local/lib/vertica-jdbc-7.2.3-0.jar https://my.vertica.com/client_drivers/7.2.x/7.2.3-0/vertica-jdbc-7.2.3-0.jar

# Install python deps.
ADD requirements.txt /app/requirements.txt
RUN pip3 install --upgrade pip \
    && pip3 install -r /app/requirements.txt \
    && rm -rf ~/.cache/pip

ENV CLASSPATH=/usr/local/share/py4jdbc/py4jdbc-assembly-0.1.6.8.jar:/usr/local/lib/vertica-jdbc-7.2.3-0.jar
ENV SPARK_HOME=/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYTHONPATH="$PYTHONPATH:$SPARK_HOME/python:/usr/local/src/squark"

WORKDIR /usr/local/src/squark
ADD docker-entrypoint.sh / 
ENTRYPOINT ["/docker-entrypoint.sh"]
