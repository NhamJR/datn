from confluent_kafka import Producer
import socket
from itertools import repeat
import time as t
from vnstock import *  # import all functions
import multiprocessing as mp
import pandas as pd
from prefect import flow, task
import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import logging
from dotenv import load_dotenv
import os
from prefect.client.schemas.schedules import IntervalSchedule
logging.basicConfig(
    filename="logging.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filemode="w",
)

# Creating an object
logger = logging.getLogger()
load_dotenv("/home/nhamzz/datn/all.env")
# conf = {'bootstrap.servers': os.environ["CONFLUENT_BOOTSTRAP_SERVER"],
#         'security.protocol': 'SASL_SSL',
#         'sasl.mechanism': 'PLAIN',
#         'sasl.username': os.environ["CONFLUENT_USERNAME"],
#         'sasl.password': os.environ["CONFLUENT_PASSWORD"],
#         'client.id': socket.gethostname()}
'''
conf = {
    "bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "HGLHHLIGH5YQYKVX",
    "sasl.password": "gX5Smh7m7hoFTvIxUGPL9hwNJmgo1nQZBr/nHpFXD56jNm52m8i5C5Dor0/XMiD9",
    "client.id": socket.gethostname(),
}
'''
conf = {
    "bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "KDTB5F5MMOC6KNZM",  # API key
    "sasl.password": "dNMPo1y5msUnZ9nI106CgH3r2GVWR6CqHy79h6GtOAb8itAxiaOLkFFctfAQmq5s",  # API Secret 
    "client.id": socket.gethostname(),
}

producer = Producer(conf)
kafka_topic = "stockPrices"
now = datetime.now()
today = str(now.date())



stock_symbols = pd.read_csv("company.csv")["ticker"].tolist()


def delivery_report(err, msg):
    if err is not None:
        logger.error("Message delivery failed: {}".format(err))
    else:
        logger.info("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def send_to_kafka(producer, topic, key, message):
    # Sending a message to Kafka
    producer.produce(
        topic,
        key=key,
        value=json.dumps(message).encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush()


def retrieve_real_time_data(stock_symbols):
    df = pd.DataFrame()
    for symbol_index, stock_symbol in enumerate(stock_symbols):
        try:
            real_time_data = stock_historical_data(
                symbol=stock_symbol,
                start_date=today,
                end_date=today,
                resolution="1D",
                type="stock",
                beautify=False,
                decor=False,
                source="DNSE",
            )
            if real_time_data is not None and not real_time_data.empty:
                df = df._append(real_time_data.iloc[-1])
        except Exception as e:
            logger.error(f"Error processing stock symbol {stock_symbol}: {str(e)}")
            continue

    return df


def divide_list(input_list, num_sublists):
    sublist_length = math.ceil(len(input_list) / num_sublists)
    return [
        input_list[i : i + sublist_length]
        for i in range(0, len(input_list), sublist_length)
    ]


@task
def collect_data():
    num_of_thread = 10
    with ThreadPoolExecutor() as pool:
        return list(
            pool.map(retrieve_real_time_data, divide_list(stock_symbols, num_of_thread))
        )


@task
def transform_data(data):
    df = pd.concat(data)
    if df.empty:
        logger.info("There is no data")
        return df
    print(len(df))
    df.loc[:, "time"] = (
        pd.to_datetime(df.loc[:, "time"])
        .dt.tz_localize("Asia/Ho_Chi_Minh")
        .values.astype(np.int64)
    )
    # ts_25min_ago = int((now-timedelta(minutes=25)).timestamp())
    # df=df.loc[df['time']>ts_25min_ago]
    df["type"] = "stock_daily"
    logger.info("Transform data successfully")
    return df


@task
def load_data(df):
    for index, row in df.iterrows():
        row = row.to_dict()
        send_to_kafka(
            producer=producer, topic=kafka_topic, key=row["ticker"], message=row
        )
        logger.info("Send data to kafka successfully")


@flow(name="dailyProducer")
def main1():
    data = collect_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)


from prefect.deployments import Deployment

schedule = IntervalSchedule(interval=timedelta(minutes=15))
deployment = Deployment.build_from_flow(
    flow=main1,
    name="daily-producer-deployment",
    schedules=[schedule],  
)



if __name__ == "__main__":
    deployment.apply()