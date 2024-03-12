# TODO: trans_id или transaction_id

import psycopg2
import pandas as pd
import os
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


def file2sql(path, table_name, schema_name, format):
    # Функция загружает файл в таблицу
    # path - путь до папки с файлом
    # table_name - имя таблицы
    # schema_name - имя схемы
    # format - csv или xlsx
    engine = create_engine(
        f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}'
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
    for table, format in zip(
        ['transactions', 'passport_blacklist', 'terminals'],
        ['csv', 'xlsx', 'xlsx']
    ):
        if format == 'csv':
            file_name = f'{path}/{table}_{date}.txt'
        elif format == 'xlsx':
            file_name = f'{path}/{table}_{date}.xlsx'

        file2sql(file_name, f'stg_{table}', 'myproject', format)
        os.rename(file_name, file_name + '.backup')


def init_stg():
    query = '''
    CREATE TABLE if not exists stg_passport_blacklist(
        passport_num varchar(128),
        entry_dt date
    );

    CREATE TABLE if not exists stg_transactions(
        trans_id varchar(128),
        trans_date date,
        card_num varchar(128),
        oper_type varchar(128),
        amt decimal,
        oper_result varchar(128),
        terminal varchar(128)
    );

    CREATE TABLE if not exists stg_terminals(
        terminal_id varchar(128),
        terminal_type varchar(128),
        terminal_city varchar(128),
        terminal_address varchar(128)
    );
    '''
    cursor.execute(query)
    conn.commit()


def init_fact_pb():
    # Инициализация dwh_fact_passport_blacklist
    query = '''
    CREATE TABLE if not exists dwh_fact_passport_blacklist(
        passport_num varchar(128),
        entry_dt date,
        create_dt timestamp default current_timestamp,
        update_dt timestamp default current_timestamp
    )
    '''
    cursor.execute(query)
    conn.commit()


def init_fact_trans():
    # Инициализация dwh_fact_transactions
    query = '''
    CREATE TABLE if not exists dwh_fact_transactions(
        trans_id varchar(128),
        trans_date date,
        card_num varchar(128),
        oper_type varchar(128),
        amt decimal,
        oper_result varchar(128),
        terminal varchar(128),
        create_dt timestamp default current_timestamp,
        update_dt timestamp default current_timestamp
        );
    '''
    cursor.execute(query)
    conn.commit()


def init_terminals_hist():
    # Инициализация dwh_dim_terminals_hist
    query = '''
    CREATE TABLE if not exists dwh_dim_terminals_hist(
        terminal_id varchar(128),
        terminal_type varchar(128),
        terminal_city varchar(128),
        terminal_address varchar(128),
        effective_from timestamp default current_timestamp,
        effective_to timestamp default timestamp '2999-12-31 23:59:59',
        deleted_flg integer default 0
    );
    '''
    cursor.execute(query)
    conn.commit()


def create_stg_new(stg_table, scd2_table, on):
    # Создает stg таблицы с новыми данными
    query = f'''
        CREATE TABLE {stg_table}_new as
            SELECT t1.*
            FROM {stg_table} t1
            LEFT JOIN {scd2_table} t2 
            ON t1.{on} = t2.{on}
            WHERE t2.{on} is NULL;
    '''
    cursor.execute(query)
    conn.commit()


def create_stg_changed():
    # Создает STG таблицу с измененными терминалами
    # Только терминалы могут меняться
    query = '''
    CREATE TABLE stg_terminals_changed as
        SELECT t1.*
        FROM stg_terminals t1
        INNER JOIN dwh_dim_terminals_hist t2
        ON t1.terminal_id = t2.terminal_id
        AND (
        t1.terminal_city <> t2.terminal_city OR
        t1.terminal_address <> t2.terminal_address
        );
    '''
    cursor.execute(query)
    conn.commit()


def create_stg_deleted():
    query = '''
        CREATE TABLE stg_terminals_deleted as
        SELECT t1.*
        FROM dwh_dim_terminals_hist t1
        LEFT JOIN stg_terminals t2
        ON t1.terminal_id = t2.terminal_id
        WHERE t2.terminal_id is NULL;
    '''
    cursor.execute(query)
    conn.commit()


def merge_stg_new():
    # TODO: исправить entry_dt
    query = f'''
        INSERT INTO dwh_fact_passport_blacklist(
            passport_num,
            entry_dt,
            create_dt,
            update_dt
        ) SELECT
            passport_num,
            entry_dt,
            current_timestamp,
            current_timestamp
        FROM stg_passport_blacklist_new;
    '''
    cursor.execute(query)

    query = f'''
    INSERT INTO dwh_fact_transactions(
        trans_id,
        trans_date,
        card_num,
        oper_type,
        amt,
        oper_result,
        terminal,
        create_dt,
        update_dt
    ) SELECT 
        trans_id,
        trans_date,
        card_num,
        oper_type,
        amt,
        oper_result,
        terminal,
        current_timestamp,
        current_timestamp
    FROM stg_transactions_new;
    '''
    cursor.execute(query)

    # Добавление новых записей
    query = f'''
    INSERT INTO dwh_dim_terminals_hist(
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        effective_from,
        effective_to,
        deleted_flg
    ) SELECT
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        current_timestamp,
        timestamp '2999-12-31 23:59:59',
        0
        FROM stg_terminals_new;
    '''
    cursor.execute(query)
    conn.commit()


def merge_stg_changed():
    # Обновление измененных данных
    query = '''
        UPDATE dwh_dim_terminals_hist
        SET terminal_city = t2.terminal_city,
            terminal_address = t2.terminal_address
        FROM dwh_dim_terminals_hist t1
            INNER JOIN stg_terminals_changed t2
                ON t1.terminal_id = t2.terminal_id
    '''
    cursor.execute(query)
    conn.commit()


def merge_stg_deleted():
    # Обновление удаленных данных
    query = '''
        UPDATE dwh_dim_terminals_hist
        SET deleted_flg = 0,
            effective_to = current_timestamp
        WHERE terminal_id IN (SELECT terminal_id FROM stg_terminals_deleted);
    '''
    cursor.execute(query)
    conn.commit()


def etl(path, date):
    # Загрузка файлов в исторические таблицы
    # path - путь до файлов
    # date - дата на которую нужно загрузить
    init_stg()
    load_data_from_files(path, date)

    init_fact_pb()
    init_fact_trans()
    init_terminals_hist()

    for stg_table, scd2_table, on in zip(
        ['stg_transactions', 'stg_passport_blacklist', 'stg_terminals'],
        ['dwh_fact_transactions', 'dwh_fact_passport_blacklist', 'dwh_dim_terminals_hist'],
        ['trans_id', 'passport_num', 'terminal_id']
    ):
        create_stg_new(stg_table, scd2_table, on)

    create_stg_changed()
    create_stg_deleted()

    merge_stg_new()
    merge_stg_changed()
    merge_stg_deleted()

    tables = [
        'stg_transactions',
        'stg_transactions_new',
        'stg_passport_blacklist',
        'stg_passport_blacklist_new',
        'stg_terminals',
        'stg_terminals_new',
        'stg_terminals_changed',
        'stg_terminals_deleted',
    ]

    for table in tables:
        query = f"""
        DROP TABLE IF EXISTS {table};
        """
        cursor.execute(query)

    conn.commit()


if __name__ == '__main__':
    etl('data', '03032021')
