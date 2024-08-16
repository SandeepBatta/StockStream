import mysql.connector


class Database:
    def __init__(self, host, user, password, port, database):
        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, port=port, database=database
        )
        self.cursor = self.conn.cursor()

    def store_data(self, data):
        insert_query = """
        INSERT INTO stock_prices (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(
            insert_query,
            (
                data["symbol"],
                data["timestamp"],
                data["open_price"],
                data["high_price"],
                data["low_price"],
                data["close_price"],
                data["volume"],
            ),
        )
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
