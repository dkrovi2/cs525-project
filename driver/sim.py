import argparse
import json
import sys
import logging

from driver import ApplicationState
from driver import start_publisher_and_consumers
from driver import wait_for_threads_to_join

LOG = logging.getLogger("root")
LOG.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s %(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch.setFormatter(formatter)
LOG.addHandler(ch)


class Args:
    def __init__(self):
        self.group_id = ""
        self.partition = 0
        self.topic = ""
        self.dataset_location = ""
        self.graph_type = None
        self.step = 50
        self.end = None
        self.verbose = False


def main():
    parser = argparse.ArgumentParser(description="Driver to publish CSV dataset")
    parser.add_argument("-d", "--dataset-location", help="Location of dataset", required=True)
    parser.add_argument("-t", "--topic", help="Name of the Kafka topic to publish the records", required=True)
    parser.add_argument("-p", "--partition", type=int, help="Number of partitions", required=True)
    parser.add_argument("-g", "--graph-type", help="Graph topology to use, one of [ring, fcg]", required=False)
    parser.add_argument("-s", "--step", help="count of records consumed before printing stats", type=int, required=False, default=50)
    parser.add_argument("-e", "--end", help="Count of records consumed to end the consumer", type=int, required=True)
    parser.add_argument("-v", "--verbose", help="Verbose", required=False, default=False)
    try:
        args = Args()
        parser.parse_args(sys.argv[1:], namespace=args)
        if args.verbose:
            LOG.setLevel(logging.DEBUG)
            ch.setLevel(logging.DEBUG)
        LOG.info("Arguments: {}".format(json.dumps(args.__dict__)))
        application_state = ApplicationState()
        start_publisher_and_consumers(args.topic,
                                      args.partition,
                                      args.dataset_location,
                                      application_state,
                                      args.graph_type,
                                      args.step,
                                      args.end)
        wait_for_threads_to_join(application_state)
    except Exception as e:
        print(e)
        parser.print_help()


main()
