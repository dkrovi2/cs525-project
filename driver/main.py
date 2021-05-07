from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
from river import compose
from river import linear_model
from river import metrics
from river import optim
from river import preprocessing
from river import utils
from time import sleep
from fastapi import FastAPI

import json
import numpy as np
import sys
import threading


class ApplicationState:
    def __init__(self):
        self.publisher = None
        self.consumers = None

    def set(self, publisher, consumers):
        self.publisher = publisher
        self.consumers = consumers


# Constants
bootstrap_servers = '127.0.0.1:9092'
kafka_conf = {"bootstrap.servers": bootstrap_servers}
application_state = ApplicationState()



class SynchronousSGD(optim.Optimizer):
    def __init__(self, lr=0.01, mylock=None):
        super().__init__(lr)
        self.mylock = mylock
    
    def _step(self, w, g):
        self.mylock.acquire()
        if isinstance(w, utils.VectorDict) and isinstance(g, utils.VectorDict):
            w -= self.learning_rate * g
        elif isinstance(w, np.ndarray) and isinstance(g, np.ndarray):
            w -= self.learning_rate * g
        else:
            for i, gi in g.items():
                w[i] -= self.learning_rate * gi
        self.mylock.release()
        return w


class OnlineModel:
    def __init__(self, optimizer):
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LogisticRegression(optimizer))
        self.metric = metrics.Accuracy()
        self.count = 0
    
    def train(self, message, group):
        self.count = self.count + 1
        record = json.loads(message.strip())
        x, y = record
        y_pred = self.model.predict_one(x)
        self.metric = self.metric.update(y, y_pred)
        self.model = self.model.learn_one(x, y)
        print("[{0}-{1}] {2}".format(group, self.count, self.metric))


class TopicConsumer:
    def __init__(self, group_id, topic, partition, optimizer):
        self.model = OnlineModel(optimizer)
        self.group_id = group_id
        self.topic = topic
        self.partition = partition
        self.stop = False
        print("Started consumer [Group: {0}, Topic: {1}, Partition: {2}".format(group_id, topic, partition))
    
    def consume(self):
        consumer_kafka_conf = {"bootstrap.servers": bootstrap_servers,
                               'group.id': self.group_id,
                               'auto.offset.reset': 'smallest'}
        consumer = Consumer(consumer_kafka_conf)
        try:
            consumer.assign([TopicPartition(topic=self.topic,
                                            partition=self.partition)])
            
            while self.stop is False:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    sleep(0.2)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    self.model.train(msg.value().strip(), self.group_id)
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

    def stop(self):
        self.stop = True


def producer_callback(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    # else:
    #     print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class TopicPublisher:
    def __init__(self, topic_name, partition_count, dataset):
        self.topic_name = topic_name
        self.partition_count = partition_count
        self.dataset = dataset
        self.stop = False
        self.producer = None
    
    def init_kafka_env(self):
        admin_client = AdminClient(kafka_conf)
        topics = admin_client.list_topics().topics
        print(topics)
        if self.topic_name not in topics:
            print("{0} topic does not exist. Creating...".format(self.topic_name))
            topic_list = [NewTopic(self.topic_name,
                                   num_partitions=self.partition_count,
                                   replication_factor=1)]
            fs = admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    print("Topic {} created".format(topic))
                except Exception as e:
                    print("Failed to create topic {}: {}".format(topic, e))
            topics = admin_client.list_topics().topics
            print(topics)
    
    def publish(self):
        self.producer = Producer(kafka_conf)
        counter = 0
        with open(self.dataset) as csv_file:
            for line in csv_file:
                line.strip()
                self.producer.poll(0.2)
                counter = counter + 1
                if counter % 1000 == 0:
                    print("[{0}] {1} records streamed so far ....".format(
                        datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                        counter))
                self.producer.produce(self.topic_name,
                                      value=line.encode('utf-8'),
                                      callback=producer_callback,
                                      partition=counter % self.partition_count)
        self.producer.flush()

    def stop(self):
        self.stop = True


class Args:
    def __init__(self):
        self.topic = ""
        self.partition = 0
        self.dataset_location = ""
        self.group_id = ""


def main(topic, partition_count, dataset_location):
    try:
        publisher = TopicPublisher(topic, partition_count, dataset_location)
        publisher.init_kafka_env()
        
        t_pub = threading.Thread(target=publisher.publish())
        print("Starting publisher...")
        t_pub.start()
        print("Starting consumers...")

        consumers = []
        t_consumers = []
        # partition numbers start with 0
        mylock = threading.Lock()
        optimizer = SynchronousSGD(0.1, mylock)
        
        for i in range(0, partition_count):
            con = TopicConsumer("test-group-{0}".format(i + 1),
                                topic,
                                i,
                                optimizer)
            t_con = threading.Thread(target=con.consume)
            print("Starting consumer-{0}...".format(i))
            t_con.start()
            t_consumers.append(t_con)
            consumers.append(con)

        application_state.set(publisher, consumers)

        t_pub.join()
        print("Publisher completed....")
        for t_con in t_consumers:
            t_con.join()

    except Exception as e:
        print(e)


app = FastAPI()


@app.get("/run/{topic}/{partition_count}/{dataset_location}")
def run(topic, partition_count, dataset_location):
    main(topic, partition_count, dataset_location)


@app.get("/stop-publisher")
def stop_publisher():
    if application_state.publisher is not None:
        application_state.publisher.stop()


@app.get("/stop-consumers")
def stop_consumers():
    if application_state.consumers is not None:
        for consumer in application_state.consumers:
            consumer.stop()

