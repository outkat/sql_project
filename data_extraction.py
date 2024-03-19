# В этом модуле происходит извлечение данных из файлов в stg таблицы
import os
import pandas as pd
from sqlalchemy import create_engine
from connection import config


def file2sql(path, table_name, schema_name, format):
    # Функция загружает файл в таблицу
    # path - путь до папки с файлом
    # table_name - имя таблицы
    # schema_name - имя схемы
    # format - csv или xlsx
    engine = create_engine(
        f'postgresql://{config["user"]}:{config["password"]
                                         }@{config["host"]}:{config["port"]}/{config["database"]}'
    )

    if format == 'csv':
        df = pd.read_csv(path, sep=';')

    elif format == 'xlsx':
        df = pd.read_excel(path)

    if table_name == 'stg_transactions':
        df = df.rename(columns={
            'transaction_id': 'trans_id',
            'transaction_date': 'trans_date',
            'amount': 'amt',

        })
        df['amt'] = df['amt'].map(lambda x: float(x.replace(',', '.')))

    elif table_name == 'stg_passport_blacklist':
        df = df.rename(columns={
            'date': 'entry_dt',
            'passport': 'passport_num'
        })

    df.to_sql(name=table_name, con=engine, schema=schema_name,
              if_exists="append", index=False)


def load_data_from_files(path, date):
    # Функция загружает все файлы в stg таблицы
    # path - путь до папки с файлами
    # date - дата выгрузки ddmmyyyy
    if not os.path.exists('archive'):
        os.mkdir('archive')

    for table, format in zip(
        ['transactions', 'passport_blacklist', 'terminals'],
        ['csv', 'xlsx', 'xlsx']
    ):
        if format == 'csv':
            file_name = f'{path}/{table}_{date}.txt'
            dst_name = f'archive/{table}_{date}.txt'
        elif format == 'xlsx':
            file_name = f'{path}/{table}_{date}.xlsx'
            dst_name = f'archive/{table}_{date}.xlsx'

        file2sql(file_name, f'stg_{table}', 'myproject', format)
        os.rename(file_name, dst_name + '.backup')
