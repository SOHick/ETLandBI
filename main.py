import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from psycopg2 import OperationalError
import pandas as pd

adult = pd.read_csv("data/netflix_titles.csv", encoding='latin1')
adult.drop(adult.columns[12:], axis=1, inplace=True)

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection