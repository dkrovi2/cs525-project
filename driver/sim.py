import argparse
import json
import threading
import sys

from main import TopicPublisher, SynchronousSGD, TopicConsumer


class Args:
    def __init__(self):
        self.group_id = ""
        self.partition = 0
        self.topic = ""
        self.dataset_location = ""


def main():
    parser = argparse.ArgumentParser(description="Driver to publish CSV dataset")
    parser.add_argument("-d", "--dataset-location", help="Location of dataset", required=True)
    parser.add_argument("-t", "--topic", help="Name of the Kafka topic to publish the records", required=True)
    parser.add_argument("-p", "--partition", type=int, help="Number of partitions", required=True)
    parser.add_argument("-g", "--group-id", help="Group ID of the consumer", required=True)
    try:
        args = Args()
        parser.parse_args(sys.argv[1:], namespace=args)
        print(json.dumps(args.__dict__))
        publisher = TopicPublisher(args.topic, args.partition, args.dataset_location)
        publisher.init_kafka_env()

        t_pub = threading.Thread(target=publisher.publish())
        print("Starting publisher...")
        t_pub.start()
        print("Starting consumers...")

        t_consumers = []
        mylock = threading.Lock()
        optimizer = SynchronousSGD(0.1, mylock)

        # partition numbers start with 0
        for i in range(0, args.partition):
            con = TopicConsumer(args.group_id + "-{0}".format(i + 1), args.topic, i, optimizer)
            t_con = threading.Thread(target=con.consume)
            print("Starting consumer-{0}...".format(i))
            t_con.start()
            t_consumers.append(t_con)

        t_pub.join()
        for t_con in t_consumers:
            t_con.join()

    except Exception as e:
        print(e)
        parser.print_help()


main()
