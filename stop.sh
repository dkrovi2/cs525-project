#!/usr/bin/env sh

PROJECT_HOME=`pwd`
KAFKA_HOME="${PROJECT_HOME}/kafka_2.13-2.7.0"
LOG_DIR="${PROJECT_HOME}/logs"

echo
echo "Stopping zookeeper ...."
echo
$KAFKA_HOME/bin/zookeeper-server-stop.sh > $LOG_DIR/zookeeper.log 2>&1

echo
echo "Stopping kafka ...."
echo
$KAFKA_HOME/bin/kafka-server-stop.sh > $LOG_DIR/kafka.log 2>&1

