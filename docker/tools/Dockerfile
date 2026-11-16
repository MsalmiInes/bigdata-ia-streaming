# ============================================================
# Image "bigdata-tools" : Hadoop 3.3.6 (HDFS + YARN + MapReduce)
# + Pig 0.17.0 + Sqoop 1.4.7 + Flume 1.11.0 + connecteur MySQL.
# Meme image pour les daemons Hadoop ET pour le conteneur "tools".
# ============================================================
FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG PIG_VERSION=0.17.0
ARG SQOOP_VERSION=1.4.7
ARG FLUME_VERSION=1.11.0
ARG MYSQL_CONNECTOR_VERSION=8.0.28
ARG APACHE=https://archive.apache.org/dist
ARG MAVEN=https://repo1.maven.org/maven2

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates procps netcat-openbsd less nano iputils-ping \
 && rm -rf /var/lib/apt/lists/*

# ---- Hadoop
RUN curl -fsSL ${APACHE}/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -xz -C /opt \
 && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm -rf /opt/hadoop/share/doc

# ---- Pig
RUN curl -fsSL ${APACHE}/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz | tar -xz -C /opt \
 && ln -s /opt/pig-${PIG_VERSION} /opt/pig

# ---- Sqoop (+ driver MySQL + commons-lang requis avec Hadoop 3)
RUN curl -fsSL ${APACHE}/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz | tar -xz -C /opt \
 && ln -s /opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0 /opt/sqoop \
 && curl -fsSL -o /opt/sqoop/lib/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar \
      ${MAVEN}/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar \
 && curl -fsSL -o /opt/sqoop/lib/commons-lang-2.6.jar \
      ${MAVEN}/commons-lang/commons-lang/2.6/commons-lang-2.6.jar

# ---- Flume (guava 11 de Flume est en conflit avec celui de Hadoop 3 : on le retire)
RUN curl -fsSL ${APACHE}/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz | tar -xz -C /opt \
 && ln -s /opt/apache-flume-${FLUME_VERSION}-bin /opt/flume \
 && rm -f /opt/flume/lib/guava-*.jar

RUN mkdir -p /opt/empty /hadoop /work

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_COMMON_HOME=/opt/hadoop \
    HADOOP_HDFS_HOME=/opt/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    HADOOP_YARN_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/etc/hadoop-conf \
    PIG_HOME=/opt/pig \
    PIG_CLASSPATH=/etc/hadoop-conf \
    SQOOP_HOME=/opt/sqoop \
    FLUME_HOME=/opt/flume \
    HBASE_HOME=/opt/empty HCAT_HOME=/opt/empty ACCUMULO_HOME=/opt/empty ZOOKEEPER_HOME=/opt/empty \
    HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root \
    HADOOP_USER_NAME=root
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/pig/bin:/opt/sqoop/bin:/opt/flume/bin:${PATH}

WORKDIR /work
CMD ["sleep", "infinity"]
