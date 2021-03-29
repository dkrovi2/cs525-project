from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from time import sleep

import argparse
import json
import sys


bootstrap_servers = '127.0.0.1:9092'


'''
Arguments
'''


class Args:
    def __init__(self):
        self.group_id = ""
        self.topic = ""
        self.partition = 0


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
        consume(args.group_id, args.topic, args.partition)
    except Exception as e:
        print(e)
        parser.print_help()
        

def consume(group_id, topic, partition):
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
                message_handler(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def message_handler(topic_msg):
    # This is the place where the model should be fed the incoming message.
    print(topic_msg.value())


# Start main method
main()
