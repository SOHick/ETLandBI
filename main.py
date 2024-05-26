import clickhouse_driver
import pandas as pd
from datetime import datetime
import psycopg2
from clickhouse_driver import Client
from sqlalchemy import create_engine

adult = pd.read_csv("data/netflix_titles.csv", encoding='latin1')
adult.drop(adult.columns[12:], axis=1, inplace=True)

pg_secret = {
    'db_host': 'localhost',
    'db_name': 'postgres',
    'db_password': 'root',
    'db_port': '5432',
    'db_user': 'postgres'
}
ch_secret = {
    'db_host': 'localhost',
    'db_name': 'default',
    'db_password': 'root',
    'db_port': '8123',
    'db_user': 'localhost'
}


def transfer_data():
    # Connect to PostgreSQL
    connection_pg = psycopg2.connect(
        database=pg_secret['db_name'],
        user=pg_secret['db_user'],
        password=pg_secret['db_password'],
        host=pg_secret['db_host'],
        port=pg_secret['db_port']
    )
    pg_cursor = connection_pg.cursor()

    # Connect to ClickHouse
    # ch_client = Client('localhost')
    connection_ch = clickhouse_driver.connect(
        database=ch_secret['db_name'],
        user=ch_secret['db_user'],
        password=ch_secret['db_password'],
        host=ch_secret['db_host'],
        port=ch_secret['db_port'])

    # Выборка данных из PostgreSQL
    pg_cursor.execute("SELECT * FROM " + '"dataNetflix"' + ";")
    rows = pg_cursor.fetchall()

    # Запись данных в ClickHouse
    connection_ch.execute("INSERT INTO dataNetflixCH VALUES", rows)

    # Закрытие соединений
    pg_cursor.close()
    connection_pg.close()
    connection_ch.disconnect()

def ch_alchemy():
    client = Client(host='localhost', port=9000)
    client.execute('SHOW TABLES;')


ch_alchemy()


