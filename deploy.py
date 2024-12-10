'''from prefect.deployments import Deployment
from prefect.client.schemas.schedules import IntervalSchedule
from producer import *
schedule = IntervalSchedule(interval=timedelta(minutes=15))

deployment = Deployment.build_from_flow(
    flow=main,
    name="producer-deployment",
    schedule=schedule,  
)
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
    logger = get_run_logger()
    for index, row in df.iterrows():
        row = row.to_dict()
        send_to_kakfa(
            producer=producer,
            topic=topic,
            key=row["ticker"],
            message=row,
            logger=logger,
        )
        logger.info("Send data to Kafka successfully")

@flow(name="producer")
def main():
    data = collect_data()
    tranformed_data = tranform_data(data)
    load_data(tranformed_data)



if __name__ == "__main__":
    deployment.apply()
'''