from influxdb_client import InfluxDBClient, Point, WritePrecision

# Thay đổi các thông số kết nối theo nhu cầu của bạn
url = "https://us-central1-1.gcp.cloud2.influxdata.com"  # Địa chỉ InfluxDB
token = "test_connect"  # Token truy cập       ElCYkRbhnVeuSH5efUEClNKgkTeMabYvxyucH2h93L3zcVgv4zyyL8z-_5X64dRbZidEfvC3bxrDlZy0LZE7Uw==
org = "datn"  # Tên tổ chức
bucket = "test_connect"  # Tên bucket

# Dữ liệu từ Kafka
key = "NT2"
value = {
    "time": 1731985200000000000,
    "open": 19100,
    "high": 19150,
    "low": 19100,
    "close": 19100,
    "volume": 5000,
    "ticker": "NT2",
    "type": "stock_price"
}

# Tạo client InfluxDB
client = InfluxDBClient(url=url, token=token, org=org)

# Tạo một điểm dữ liệu từ giá trị JSON
point = (
    Point("stock_price")  # Tên measurement
    .tag("ticker", value["ticker"])  # Tag cho ticker
    .tag("type", value["type"])  # Tag cho type
    .field("open", value["open"])  # Field cho open
    .field("high", value["high"])  # Field cho high
    .field("low", value["low"])  # Field cho low
    .field("close", value["close"])  # Field cho close
    .field("volume", value["volume"])  # Field cho volume
    .time(value["time"], WritePrecision.NS)  # Timestamp
)

# Ghi điểm dữ liệu vào bucket
write_api = client.write_api()
try:
    write_api.write(bucket=bucket, org=org, record=point)
    print("Dữ liệu đã được ghi thành công!")
except Exception as e:
    print(f"Lỗi khi ghi dữ liệu: {e}")

# Đóng kết nối
client.close()