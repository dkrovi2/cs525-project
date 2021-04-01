# Online Machine Learning: Driver & Consumer

## Driver

This module publishes the records in a CSV data set, onto a kafka topic, with the specified number of partitions.

**Input**:

1. Location of CSV records
1. Name of the kafka topic
1. Number of partitions

## Consumer

This module consumes CSV records from the partition of a specified kafka topic and processes it.

**Input**:

1. Name of the consumer group
1. Name of the kafka topic
1. Partition number

# How to build and run the commands

This is a Poetry managed python project. 

## Build

The following command builds the project:

    $ poetry build

## Run Driver

The pre-requisite to running the driver and consumer is to start kafka and zookeeper processes. 

This can be done by executing the following command in the parent directory:

    $ sh start.sh

The following command inside the poetry shell starts the driver program

    $ poetry run python3 driver.py -d <location of dataset> -t topic -p partition_count

The following command inside the poetry shell starts the consumer program

    $ poetry run python3 consumer.py -g <group-id> -t topic -p partition_number


Partition numbers start with *0* to *n-1* for *n* partitions.

**Note:**

Make sure to stop the Kafka and Zookeeper services once done using the following command:

    $ sh stop.sh
