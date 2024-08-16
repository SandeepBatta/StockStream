import json


# read config from json file
def load_config(file="config.json"):
    with open(file, "r") as f:
        config = json.load(f)
    return config


# Read kafka client properties
def read_kafka_config(file="client.properties"):
    config = {}
    with open(file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config


def get_symbols():
    return ["AAPL", "GOOGL", "AMZN", "MSFT", "FB"]


def get_api_key():
    config = load_config()
    return config["ALPHA_VANTAGE_API_KEY"]


def get_db_config():
    config = load_config()
    return {
        "host": config["DB_HOST"],
        "user": config["DB_USER"],
        "password": config["DB_PASSWORD"],
        "port": config["DB_PORT"],
        "database": config["DB_NAME"],
    }
