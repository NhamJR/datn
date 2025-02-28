import threading
from confluent_kafka import Consumer, KafkaError
import socket
import logging
from influx_db import (
    message_queue,
    start_worker_threads,
    shutdown_workers,
    close_influxdb_client,
)
import json

consumer = Consumer(
    {
        "bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "KDTB5F5MMOC6KNZM",
        "sasl.password": "dNMPo1y5msUnZ9nI106CgH3r2GVWR6CqHy79h6GtOAb8itAxiaOLkFFctfAQmq5s",
        "group.id": "stock_price_group",
        "auto.offset.reset": "latest",  
        "client.id": socket.gethostname(),
    }
)

consumer.subscribe(["stockPrices"])
rx_logger = logging.getLogger("Rx")
rx_logger.setLevel(logging.WARNING)


def consume_messages():
    while True:
        msg = consumer.poll(10)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error while consuming: {msg.error()}")
        else:
            # push_data(json.loads(msg.value().decode("utf-8")))
            message_queue.put(json.loads(msg.value().decode("utf-8")))


def main():
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.daemon = True
    consumer_thread.start()

    worker_threads = start_worker_threads(num_threads=10)

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Received exit signal, shutting down...")

    shutdown_workers(worker_threads)
    consumer.close()
    close_influxdb_client()


if __name__ == "__main__":
    main()

