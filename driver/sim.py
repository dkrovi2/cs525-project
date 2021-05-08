import argparse
import json
import sys

from driver import ApplicationState
from driver import start_publisher_and_consumers
from driver import wait_for_threads_to_join


class Args:
    def __init__(self):
        self.group_id = ""
        self.partition = 0
        self.topic = ""
        self.dataset_location = ""
        self.graph_type = None


def main():
    parser = argparse.ArgumentParser(description="Driver to publish CSV dataset")
    parser.add_argument("-d", "--dataset-location", help="Location of dataset", required=True)
    parser.add_argument("-t", "--topic", help="Name of the Kafka topic to publish the records", required=True)
    parser.add_argument("-p", "--partition", type=int, help="Number of partitions", required=True)
    parser.add_argument("-g", "--graph-type", help="Graph topology to use, one of [ring, fcg[", required=False)
    try:
        args = Args()
        parser.parse_args(sys.argv[1:], namespace=args)
        print(json.dumps(args.__dict__))
        application_state = ApplicationState()
        start_publisher_and_consumers(args.topic,
                                      args.partition,
                                      args.dataset_location,
                                      application_state,
                                      args.graph_type)
        wait_for_threads_to_join(application_state)
    except Exception as e:
        print(e)
        parser.print_help()


main()
