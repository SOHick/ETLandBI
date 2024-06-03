import pandas as pd
import psycopg2
from clickhouse_driver import Client
from sqlalchemy import create_engine

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

df = pd.read_csv("data/cars_23.csv", encoding='latin1', delimiter=',', header=0)
def push_PostgreSQL():
    df = pd.read_csv("data/cars_23.csv", encoding='latin1', delimiter=',', header=0)
    df = df.rename(columns={df.columns[0]: 'id'})
    df = df.rename(columns={df.columns[1]: 'car_name'})
    engine = create_engine(
        f'postgresql+psycopg2://{pg_secret["db_user"]}:{pg_secret["db_password"]}@{pg_secret["db_host"]}:{pg_secret["db_port"]}/{pg_secret["db_name"]}')
    df.to_sql('datacar', engine, index=False, if_exists='replace')


def transfer_data():
    # Подключение к PostgreSQL
    connection_pg = psycopg2.connect(
        database=pg_secret['db_name'],
        user=pg_secret['db_user'],
        password=pg_secret['db_password'],
        host=pg_secret['db_host'],
        port=pg_secret['db_port']
    )
    pg_cursor = connection_pg.cursor()

    # Подключение к ClickHouse
    client_ch = Client(host='localhost', port=9000, password='root')

    # Выборка данных из PostgreSQL
    pg_cursor.execute("SELECT * FROM datacar;")
    rows = pg_cursor.fetchall()

    # Преобразование значений "None" в NULL и year convert int
    for i in range(len(rows)):
        rows[i] = list(rows[i])
        for j in range(len(rows[i])):
            if j == 2 and rows[i][j] is not None:
                rows[i][j] = int(rows[i][j])
            if rows[i][j] is None:
                if j == 2:
                    rows[i][j] = 0
                else:
                    rows[i][j] = 'NULL'
        rows[i] = tuple(rows[i])

    # Запись данных в ClickHouse
    result = client_ch.execute(
        'INSERT INTO datacarCH (id,car_name,year,distance,owner,fuel,location,drive,type,price) VALUES',
        rows)

    # Закрытие соединений
    pg_cursor.close()
    connection_pg.close()
    client_ch.disconnect()


