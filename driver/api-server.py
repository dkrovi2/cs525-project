from fastapi import FastAPI
from driver import ApplicationState, start_publisher_and_consumers

application_state = ApplicationState()
app = FastAPI()


@app.get("/run/{topic}/{partition_count}/{dataset_location}")
def run(topic, partition_count, dataset_location):
    start_publisher_and_consumers(topic,
                                  partition_count,
                                  dataset_location,
                                  application_state)
    return "Started the publisher and consumers\n"


@app.get("/stop-publisher")
def stop_publisher():
    if application_state.publisher is not None:
        application_state.publisher.stop()


@app.get("/stop-consumers")
def stop_consumers():
    if application_state.consumers is not None:
        for consumer in application_state.consumers:
            consumer.stop()
