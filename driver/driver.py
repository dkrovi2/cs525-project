from river import optim

from dol_optim.sync_sgd import SynchronousSGD
from appl.kafka_interface import TopicConsumer, TopicPublisher

import sys
import threading
import traceback

# Constants
bootstrap_servers = '127.0.0.1:9092'
kafka_conf = {"bootstrap.servers": bootstrap_servers}


class ApplicationState:
    def __init__(self):
        self.publisher = None
        self.consumers = None
        self.pub_thread = None
        self.con_threads = None

    def set(self, publisher, consumers, t_pub, t_consumers):
        self.publisher = publisher
        self.consumers = consumers
        self.pub_thread = t_pub
        self.con_threads = t_consumers


def wait_for_threads_to_join(app_state):
    app_state.pub_thread.join()
    print("Publisher completed....")
    for t_con in app_state.con_threads:
        t_con.join()


def add_neighbors_for_ring_topology(consumers, partition_count):
    print("Using ring topology")
    # Ring topology
    for i in range(1, partition_count):
        consumers[i - 1].set_neighbors([consumers[i].model])
    consumers[partition_count - 1].set_neighbors([consumers[0].model])


def add_neighbors_for_fully_connected_topology(consumers, partition_count):
    print("Using fully-connected topology")
    # Fully-connected topology
    for i in range(0, partition_count):
        neighbors = []
        for j in range(0, partition_count):
            if i != j:
                neighbors.append(consumers[j].model)
        consumers[i].set_neighbors(neighbors)


def start_publisher_and_consumers(topic,
                                  partition_count,
                                  dataset_location,
                                  application_state,
                                  topology=None):
    try:
        publisher = TopicPublisher(topic, partition_count, dataset_location, kafka_conf)
        publisher.init_kafka_env()

        t_pub = threading.Thread(target=publisher.publish())
        print("Starting publisher...")
        t_pub.start()
        print("Starting consumers...")

        consumers = []
        t_consumers = []
        # partition numbers start with 0

        for i in range(0, partition_count):
            con = TopicConsumer("test-group-{0}".format(i + 1), topic, i, bootstrap_servers)
            consumers.append(con)

        if topology == "ring":
            add_neighbors_for_ring_topology(consumers, partition_count)
        elif topology == "fcg":
            add_neighbors_for_fully_connected_topology(consumers, partition_count)
        else:
            print("No topology configured")

        for i in range(0, partition_count):
            t_con = threading.Thread(target=consumers[i].consume)
            t_con.start()
            t_consumers.append(t_con)

        application_state.set(publisher, consumers, t_pub, t_consumers)

    except Exception:
        traceback.print_exc(file=sys.stdout)
