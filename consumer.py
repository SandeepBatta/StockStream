from confluent_kafka import Consumer
import json
from config import read_kafka_config, get_db_config
from db import Database


# Consume the data from kafka topic and push data to the database
def consume():
    config = read_kafka_config()
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    consumer = Consumer(config)
    consumer.subscribe(["stock_prices"])

    db_config = get_db_config()
    db = Database(**db_config)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                symbol = msg.key().decode("utf-8")
                value = json.loads(msg.value().decode("utf-8"))

                data = {
                    "symbol": symbol,
                    "timestamp": msg.timestamp()[1],
                    "open_price": float(value["1. open"]),
                    "high_price": float(value["2. high"]),
                    "low_price": float(value["3. low"]),
                    "close_price": float(value["4. close"]),
                    "volume": int(value["5. volume"]),
                }

                db.store_data(data)
                print(f"Stored {symbol} data to DB: {data}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    consume()
