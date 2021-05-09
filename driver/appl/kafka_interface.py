from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
from time import sleep
from appl.olm import OnlineModel

import sys
import logging

LOG = logging.getLogger("root")


class TopicConsumer:
    def __init__(self, group_id, topic, partition, bootstrap_servers, step, end, worker_id, worker_count, topology):
        self.model = OnlineModel(step, group_id)
        self.group_id = group_id
        self.topic = topic
        self.partition = partition
        self.bootstrap_servers = bootstrap_servers
        self.stop = False
        self.end = int(end)
        self.worker_count = worker_count
        self.worker_id = worker_id
        self.topology = topology

    def get_group_id(self):
        return self.group_id

    def set_neighbors(self, neighbors):
        self.model.set_neighbors(neighbors)

    def get_result(self):
        metric_log = ""
        for metric in self.model.metrics:
            metric_log = metric_log + str(metric.get()) + " | "
        return "{} | {} | {} | {}".format(self.worker_count,self.worker_id,self.topology,metric_log.strip())

    def consume(self):
        consumer_kafka_conf = {"bootstrap.servers": self.bootstrap_servers,
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
                    if self.model.count == self.end:
                        self.stop = True
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

    def stop(self):
        self.stop = True


def producer_callback(err, msg):
    if err is not None:
        LOG.info('Message delivery failed: {}, {}'.format(err, msg))


class TopicPublisher:
    def __init__(self, topic_name, partition_count, dataset, kafka_conf, step):
        self.topic_name = topic_name
        self.partition_count = partition_count
        self.dataset = dataset
        self.stop = False
        self.kafka_conf = kafka_conf
        self.producer = None
        self.step = int(step)

    def init_kafka_env(self):
        admin_client = AdminClient(self.kafka_conf)
        topics = admin_client.list_topics().topics
        if self.topic_name not in topics:
            LOG.info("{0} topic does not exist. Creating...".format(self.topic_name))
            topic_list = [NewTopic(self.topic_name,
                                   num_partitions=self.partition_count,
                                   replication_factor=1)]
            fs = admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()  # The result itself is None
                    LOG.info("Topic {} created".format(topic))
                except Exception as e:
                    LOG.info("Failed to create topic {}: {}".format(topic, e))
            topics = admin_client.list_topics().topics
            LOG.info(topics)

    def publish(self):
        LOG.debug("Streaming the data from {}".format(self.dataset))
        self.producer = Producer(self.kafka_conf)
        counter = 0
        count = int(self.partition_count)
        with open(self.dataset) as csv_file:
            for line in csv_file:
                line.strip()
                self.producer.poll(0.2)
                counter = counter + 1
                if counter % self.step == 0:
                    LOG.info("{} records streamed so far ....".format(counter))
                self.producer.produce(self.topic_name,
                                      value=line.encode('utf-8'),
                                      callback=producer_callback,
                                      partition=counter % count)
        self.producer.flush()

    def stop(self):
        self.stop = True

