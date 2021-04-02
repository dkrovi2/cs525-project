from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from time import sleep

from river import compose
from river import linear_model
from river import metrics
from river import preprocessing

import argparse
import json
import sys
import traceback

bootstrap_servers = '127.0.0.1:9092'

'''
Arguments
'''


class Args:
    def __init__(self):
        self.group_id = ""
        self.topic = ""
        self.partition = 0


class Model:
    def __init__(self):
        self.model = compose.Pipeline(preprocessing.StandardScaler(), linear_model.LogisticRegression())
        self.metric = metrics.Accuracy()

    def train(self, message):
        print("Message: {0}".format(message))
        record = json.loads(message.strip())
        x, y = record
        y_pred = self.model.predict_one(x)
        self.metric = self.metric.update(y, y_pred)
        self.model = self.model.learn_one(x, y)
        print("Accuracy: {0}, {1}".format(self.metric, self.model))


'''
Main method
'''


def main():
    parser = argparse.ArgumentParser(description="Driver to publish CSV dataset")
    parser.add_argument("-g", "--group-id", help="Group ID of the consumer", required=True)
    parser.add_argument("-t", "--topic", help="Name of the Kafka topic to consumer the records", required=True)
    parser.add_argument("-p", "--partition", type=int, help="Partition to poll", required=True)
    try:
        args = Args()
        parser.parse_args(sys.argv[1:], namespace=args)
        print(json.dumps(args.__dict__))
        model = Model()
        consume(args.group_id, args.topic, args.partition, model)
    except Exception as e:
        print(e)
        traceback.print_exc()
        parser.print_help()


def consume(group_id, topic, partition, model):
    kafka_conf = {"bootstrap.servers": bootstrap_servers,
                  'group.id': group_id,
                  'auto.offset.reset': 'smallest'}
    consumer = Consumer(kafka_conf)
    try:
        consumer.assign([TopicPartition(topic=topic, partition=partition)])

        while True:
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
                model.train(msg.value().strip())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


# Start main method
main()
