#!/usr/bin/env sh

PROJECT_HOME=`pwd`
KAFKA_HOME="${PROJECT_HOME}/kafka_2.13-2.7.0"
LOG_DIR="${PROJECT_HOME}/logs"

if [ ! -d "$LOG_DIR" ]
then
  echo
  echo "Logs directory does not exist. Creating: $LOG_DIR"
  echo
  mkdir -p ${LOG_DIR}
else
  echo
  echo "Logs directory exists already. NOT Creating: $LOG_DIR"
  echo
fi

echo
echo "Starting zookeeper ...."
echo
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties > $LOG_DIR/zookeeper.log 2>&1

echo
echo "Starting kafka ...."
echo
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties > $LOG_DIR/kafka.log 2>&1

