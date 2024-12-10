'''from confluent_kafka import Producer
import socket
from itertools import repeat
import time 
from vnstock import *
import multiprocessing as mp
import pandas as pd
from prefect import flow, task , get_run_logger
from prefect.context import MissingContextError
from prefect.client.schemas.schedules import IntervalSchedule
import math
from datetime import datetime, timedelta
import numpy as np
import logging 
import os
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(
    filename="logging.log",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filemode="w",
)

logger = logging.getLogger()

conf = {
    "bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "KDTB5F5MMOC6KNZM",  # API key
    "sasl.password": "dNMPo1y5msUnZ9nI106CgH3r2GVWR6CqHy79h6GtOAb8itAxiaOLkFFctfAQmq5s",  # API Secret 
    "client.id": socket.gethostname(),
}

producer = Producer(conf)

def callback_report(err, msg, logger):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else: 
        logger.info(f"Message delivery to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

topic = "kafka_topic"

now = datetime.now()    
today = str(now.date())
print(today)

producer.produce(topic, key = "test_key", value = "hello", callback = callback_report)


stock_symbols  = pd.read_csv("company.csv")["ticker"].to_list()


def send_to_kakfa(producer, topic, key, message, logger):
    producer.produce(
        topic,
        key = key,
        value = json.dumps(message).encode("utf-8"),
        callback = lambda err, msg: callback_report(err, msg, logger),
    )
    producer.flush()

def crawl_realtime_data(stock_symbols):
    df = pd.DataFrame()

    for symbol_index, stock_symbol in enumerate(stock_symbols):
        try:
            realtime_data = stock_historical_data(
                symbol=stock_symbol,
                start_date=today,
                end_date=today,
                resolution="15",
                type="stock",
                beautify="False",
                decor=False,
                source="DNSE"
            )
            if realtime_data is not None and not realtime_data.empty:
                df = pd.concat([df, realtime_data.iloc[[-1]]], ignore_index=True)
        except Exception as e:
            logger.error(f"Error processing stock symbol {stock_symbol} : {str(e)}")
            continue

    return df

    


def divide_list(input_list, num_subLists):
    subList_length = math.ceil(len(input_list) / num_subLists )
    return [
        input_list[i : i + subList_length]
        for i in range(0, len(input_list), subList_length)
    ]        



def get_logger() -> logging.Logger:
    try:
        return get_run_logger()
    except MissingContextError:
        logger = logging.getLogger("PrefectFallbackLogger")
        logger.setLevel(logging.INFO) 
        return logger


@task
def collect_data():
    num_0f_thread = 10
    with ThreadPoolExecutor() as pool:
        return list(
            pool.map(crawl_realtime_data, divide_list(stock_symbols, num_0f_thread))
        )
    
@task
def tranform_data(data):
    df = pd.concat(data)
    if df.empty:
        logger.info("There is no data")
        return df
    print(len(df))
    df.loc[:, "time"] =(
        pd.to_datetime(df.loc[:, "time"]).dt.tz_localize("Asia/Ho_Chi_Minh").values.astype(np.int64)
    )
    df["type"] = "stock_price"
    logger.info("Tranform data successfully")
    return df

@task
def load_data(df):
    logger = get_logger()
    logger.info("Starting to load data...")
    for index, row in df.iterrows():
        row = row.to_dict()
        send_to_kakfa(
            producer,
            topic,
            row["ticker"],
            row,
            logger,
        )
        logger.info("Send data to Kafka successfully")



@flow(name="producer")
def main():
    data = collect_data()
    tranformed_data = tranform_data(data)
    load_data(tranformed_data)

from prefect.deployments import Deployment

schedule = IntervalSchedule(interval=timedelta(minutes=10))
deployment = Deployment.build_from_flow(
    flow=main,
    name="producer-deployment",
    schedules=[schedule],  
)


if __name__ == "__main__":
    #main()
    deployment.apply()

'''





from confluent_kafka import Producer
import socket
from itertools import repeat
import time 
from vnstock import *
import multiprocessing as mp
import pandas as pd
from prefect import flow, task , get_run_logger
from prefect.context import MissingContextError
from prefect.client.schemas.schedules import IntervalSchedule
import math
from datetime import datetime, timedelta
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor


conf = {
    "bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "KDTB5F5MMOC6KNZM",  # API key
    "sasl.password": "dNMPo1y5msUnZ9nI106CgH3r2GVWR6CqHy79h6GtOAb8itAxiaOLkFFctfAQmq5s",  # API Secret 
    "client.id": socket.gethostname(),
}

producer = Producer(conf)

def callback_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else: 
        print(f"Message delivery to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

topic = "stockPrices"

now = datetime.now()    
today = str(now.date())
print(today)


stock_symbols  = pd.read_csv("company.csv")["ticker"].to_list()

def send_to_kakfa(producer, topic, key, message):
    producer.produce(
        topic,
        key = key,
        value = json.dumps(message).encode("utf-8"),
        callback = lambda err, msg: callback_report(err, msg),
    )
    producer.flush()

def crawl_realtime_data(stock_symbols):
    df = pd.DataFrame()

    for symbol_index, stock_symbol in enumerate(stock_symbols):
        try:
            realtime_data = stock_historical_data(
                symbol=stock_symbol,
                start_date="2024-10-01",
                end_date=today,
                resolution="15",
                type="stock",
                beautify="False",
                decor=False,
                source="DNSE"
            )
            if realtime_data is not None and not realtime_data.empty:
                df = pd.concat([df, realtime_data.iloc[[-1]]], ignore_index=True)
        except Exception as e:
            print(f"Error processing stock symbol {stock_symbol} : {str(e)}")
            continue

    return df

def divide_list(input_list, num_subLists):
    subList_length = math.ceil(len(input_list) / num_subLists )
    return [
        input_list[i : i + subList_length]
        for i in range(0, len(input_list), subList_length)
    ]        

def get_logger() -> None:
    try:
        return get_run_logger()
    except MissingContextError:
        return None

@task
def collect_data():
    num_0f_thread = 10
    with ThreadPoolExecutor() as pool:
        return list(
            pool.map(crawl_realtime_data, divide_list(stock_symbols, num_0f_thread))
        )

@task
def tranform_data(data):
    df = pd.concat(data)
    if df.empty:
        print("There is no data")
        return df
    print(len(df))
    df.loc[:, "time"] =(
        pd.to_datetime(df.loc[:, "time"]).dt.tz_localize("Asia/Ho_Chi_Minh").values.astype(np.int64)
    )
    df["type"] = "stock_price"
    print("Transform data successfully")
    return df

@task
def load_data(df):
    print("Starting to load data...")
    for index, row in df.iterrows():
        row = row.to_dict()
        send_to_kakfa(
            producer,
            topic,
            row["ticker"],
            row,
        )
        print("Send data to Kafka successfully")

@flow(name="producer")
def main():
    data = collect_data()
    tranformed_data = tranform_data(data)
    load_data(tranformed_data)

from prefect.deployments import Deployment

schedule = IntervalSchedule(interval=timedelta(minutes=15))
deployment = Deployment.build_from_flow(
    flow=main,
    name="producer-deployment",
    schedules=[schedule],  
)

if __name__ == "__main__":
    #main()
    deployment.apply()

