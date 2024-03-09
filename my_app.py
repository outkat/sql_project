import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine 

conf = {
    "host": "localhost",
    "database": "sbx",
    "user": "postgres",
    "password": "postgres",
    "port": "5432",
    "options": "-c search_path=myproject"
}

conn = psycopg2.connect(**conf)
cursor = conn.cursor()

cursor.execute("create schema if not exists myproject")
conn.commit()

def csv2sql(path, conf, table_name, schema_name):
    engine = create_engine(f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
    df = pd.read_csv(path)
    df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists="replace", index=False)


def init_tables(path):
    with open(path, 'r') as f:
        data = f.read()
    
    try: 
        cursor.execute(data)
        conn.commit()
    except psycopg2.errors.Error as e:
        print(f'Warning! {e}')

if __name__ == '__main__':
    init_tables('ddl_dml.sql')
