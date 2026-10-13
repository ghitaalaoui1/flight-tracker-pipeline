#!/bin/bash
# Télécharge les jars du connecteur Kafka pour Spark dans /opt/jars.
# Lancé automatiquement par docker/spark.Dockerfile pendant le build.
set -e

SPARK_VERSION=3.5.0
SCALA_VERSION=2.12
KAFKA_VERSION=3.5.0
POOL_VERSION=2.11.1
BASE=https://repo1.maven.org/maven2

mkdir -p /opt/jars
cd /opt/jars

curl -fsSLO $BASE/org/apache/spark/spark-sql-kafka-0-10_$SCALA_VERSION/$SPARK_VERSION/spark-sql-kafka-0-10_$SCALA_VERSION-$SPARK_VERSION.jar
curl -fsSLO $BASE/org/apache/spark/spark-token-provider-kafka-0-10_$SCALA_VERSION/$SPARK_VERSION/spark-token-provider-kafka-0-10_$SCALA_VERSION-$SPARK_VERSION.jar
curl -fsSLO $BASE/org/apache/kafka/kafka-clients/$KAFKA_VERSION/kafka-clients-$KAFKA_VERSION.jar
curl -fsSLO $BASE/org/apache/commons/commons-pool2/$POOL_VERSION/commons-pool2-$POOL_VERSION.jar

ls -1 /opt/jars
