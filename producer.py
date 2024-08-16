from confluent_kafka import Producer
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor
from config import read_kafka_config, get_symbols, get_api_key


# fetch data from alphavantage every min and push data to kafka
def fetch_stock_data(symbol, producer, topic, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}"
    latest_timestamp = None

    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            time_series = data.get("Time Series (1min)", {})

            for timestamp, values in sorted(time_series.items()):
                timestamp_ms = int(
                    time.mktime(time.strptime(timestamp, "%Y-%m-%d %H:%M:%S")) * 1000
                )

                if latest_timestamp is None or timestamp_ms > latest_timestamp:
                    value = json.dumps(values)
                    producer.produce(
                        topic, key=symbol, value=value, timestamp=timestamp_ms
                    )
                    print(
                        f"Produced {symbol} message to topic {topic}: value = {value} timestamp = {timestamp_ms}"
                    )
                    latest_timestamp = timestamp_ms

        producer.flush()
        time.sleep(60)  # Fetch new data every minute


# Publish the data to kafka parallely
def produce():
    config = read_kafka_config()
    topic = "stock_prices"
    api_key = get_api_key()
    producer = Producer(config)
    symbols = get_symbols()

    with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        for symbol in symbols:
            executor.submit(fetch_stock_data, symbol, producer, topic, api_key)


if __name__ == "__main__":
    produce()
