import argparse
import json
import sys

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Constants
bootstrap_servers = '127.0.0.1:9092'
kafka_conf = {"bootstrap.servers": bootstrap_servers}

'''
Arguments
'''


class Args:
    def __init__(self):
        self.topic = ""
        self.partition = 0
        self.dataset_location = ""


'''
Main method
'''


def main():
    parser = argparse.ArgumentParser(description="Driver to publish CSV dataset")
    parser.add_argument("-d", "--dataset-location", help="Location of dataset", required=True)
    parser.add_argument("-t", "--topic", help="Name of the Kafka topic to publish the records", required=True)
    parser.add_argument("-p", "--partition", type=int, help="Number of partitions", required=True)
    try:
        args = Args()
        parser.parse_args(sys.argv[1:], namespace=args)
        print(json.dumps(args.__dict__))
        init_kafka_env(args.topic, args.partition)
        publish(args.dataset_location, args.topic, args.partition)
    except Exception as e:
        print(e)
        parser.print_help()


def init_kafka_env(topic_name, partitions):
    admin_client = AdminClient(kafka_conf)
    topics = admin_client.list_topics().topics
    print(topics)
    if not topic_name in topics:
        print("{0} topic does not exist. Creating...".format(topic_name))
        topic_list = [NewTopic(topic_name, num_partitions=partitions, replication_factor=1)]
        fs = admin_client.create_topics(topic_list)
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
        topics = admin_client.list_topics().topics
        print(topics)


def producer_callback(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def publish(dataset, topic, partitions):
    p = Producer(kafka_conf)
    counter = 0
    with open(dataset) as csv_file:
        for line in csv_file:
            line.strip()
            p.poll(0.2)
            p.produce(topic,
                      value=line.encode('utf-8'),
                      callback=producer_callback,
                      partition=counter % partitions)
            counter = counter + 1
    p.flush()


# Start execution with main method
main()
