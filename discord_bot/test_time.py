from influxdb_client import InfluxDBClient
import pandas as pd

# Kết nối đến InfluxDB
INFLUXDB_TOKEN ='01voAm-7C9y5eA-3CVyxCNSebcEklTIw6NduR60Avm-qJSYwVxfTlCpNTrEH6fqIy4-LxleESz0Lr4ytgRVqkA=='
INFLUXDB_ORG = 'datn'
INFLUXDB_BUCKET = 'stock_data'
INFLUXDB_URL = 'https://us-central1-1.gcp.cloud2.influxdata.com'

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

# Truy vấn dữ liệu từ InfluxDB
query = f"""
from(bucket: "{INFLUXDB_BUCKET}")
|> range(start: -1d)
|> filter(fn: (r) => r._measurement == "stock_price")
|> limit(n: 5)
"""

# Lấy dữ liệu dưới dạng DataFrame
df = query_api.query_data_frame(query)

# Kiểm tra kiểu dữ liệu của cột _time
print("Kiểu dữ liệu của cột _time:", df['_time'].dtype)

df.set_index("_time", inplace=True)
df.index = df.index.tz_convert("Asia/Ho_Chi_minh").strftime("%Y-%m-%d %H:%M:%S")

# Hiển thị DataFrame để kiểm tra giá trị
print(df)
