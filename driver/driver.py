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


def start_publisher_and_consumers(topic,
                                  partition_count,
                                  dataset_location,
                                  application_state,
                                  common_optimizer=False):
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
        mylock = threading.Lock()
        sync_optimizer = None
        if common_optimizer:
            print("Using a common optimizer")
            sync_optimizer = SynchronousSGD(0.1, mylock)
        else:
            print("NOT Using a common optimizer")

        for i in range(0, partition_count):
            optimizer = sync_optimizer
            if not common_optimizer:
                optimizer = optim.SGD(0.1)
            con = TopicConsumer("test-group-{0}".format(i + 1),
                                topic,
                                i,
                                bootstrap_servers,
                                optimizer)
            t_con = threading.Thread(target=con.consume)
            print("Starting consumer-{0}...".format(i))
            t_con.start()
            t_consumers.append(t_con)
            consumers.append(con)

        application_state.set(publisher, consumers, t_pub, t_consumers)

    except Exception:
        traceback.print_exc(file=sys.stdout)
