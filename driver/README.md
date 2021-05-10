# Distributed Online ML Optimizer

In this module

* `dol_optim/SyncSGD.py` implements [Distributed Autonomous Online Gradient Descent](https://arxiv.org/abs/1006.4039) algorithm
* `sim.py` simulates distributed streaming processor architecture where the data sent to multiple partitions on a kafka topic are retrieved and used to train an online ML model using the [river-ml](http://riverml.xyz) library

# How to build and run the commands

This is a Poetry managed python project. 

## Build

The following command builds the project:

    $ poetry install

## Run Driver

The pre-requisite to running the driver and consumer is to start kafka and zookeeper processes. 

This can be done by executing the following command in the parent directory:

    $ sh start.sh

The following command activates the Virtual Env where all the dependencies are installed

    $ poetry shell

The following command runs the publisher and the consumers

    $ python3 sim.py -d <location of dataset> -t topic -p partition_count -g <topology>


**Note:**
Make sure to stop the Kafka and Zookeeper services once done using the following command:

    $ sh stop.sh
