# TODO:
# 1. Загрузить файлы в STG_<> таблицы
# 2. С помощью инкрементальной загрузки обновить FACT_<>
#   таблицы с историческими данными (transactions, passport_blacklist, 
# )

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

def file2sql(path, conf, table_name, schema_name, format):
    engine = create_engine(f'postgresql://{conf["user"]}:{conf["password"]}@{conf["host"]}:{conf["port"]}/{conf["database"]}')
    if format == 'csv':
        df = pd.read_csv(path, sep=';')
    elif format == 'excel':
        df = pd.read_excel(path)
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


def load_data_from_files(path, date, format, file_name=None):
    # path - путь до папки с файлами, date - дата выгрузки ddmmyyyy
    if format == 'csv':
        file2sql(f'{path}/transactions_{date}.txt', conf, 'stg_transactions', 'myproject', 'csv')
        os.rename(f'{path}/transactions_{date}.txt', f'{path}/transactions_{date}.txt.backup')
    elif format == 'excel':
        if file_name == 'passport_blacklist':
            file2sql(f'{path}/passport_blacklist_{date}.xlsx',
                      conf, 'stg_passport_blacklist', 'myproject', 'excel')
            os.rename(f'{path}/passport_blacklist_{date}.xlsx',
                       f'{path}/passport_blacklist_{date}.xlsx.backup')
        elif file_name == 'terminals':
             file2sql(f'{path}/terminals_{date}.xlsx',
                      conf, 'stg_terminals', 'myproject', 'excel')
             os.rename(f'{path}/terminals_{date}.xlsx',
                       f'{path}/terminals_{date}.xlsx.backup')



def init_fact_pb():
    query = '''
        CREATE TABLE if not exists dwh_fact_passport_blacklist(
        passport_num varchar(128),
        entry_dt date);
    )
    '''
    cursor.execute(query)
    conn.commit()

def init_fact_trans():
    query = '''
        CREATE TABLE if not exists dwh_fact_transactions(
        trans_id varchar(128),
        trans_date date,
        card_num varchar(128),
        oper_type varchar(128),
        amt decimal,
        oper_result varchar(128),
        terminal varchar(128)
        );
    '''
    cursor.execute(query)
    conn.commit()

def init_hist_terminals():
    query = '''
        CREATE TABLE dwh_dim_terminals_hist(
        terminal_id varchar(128),
        terminal_type varchar(128),
        terminal_city varchar(128),
        terminal_adress varchar(128)
    );
    '''
    cursor.execute(query)
    conn.commit()

def scd2_merge_new(stg_table, scd2_table, on):
    query = f'''
        CREATE TABLE {stg_table}_new as
            SELECT t1.*
            FROM {stg_table} t1
            LEFT JOIN {scd2_table} t2 
            ON t1.{on} = t2.{on}
            WHERE t2.{on} is NULL;  
    )
    '''
    cursor.execute(query)
    conn.commit()


def terminals_changed():
    query = '''
        CREATE TABLE stg_terminals_changed as
            SELECT t1.*
            FROM stg_terminals t1
            INNER JOIN dwh_dim_terminals_hist t2
            ON t1.terminal_id = t2.terminal_id
            AND (
            t1.terminal_city <> t2.terminal_city,
            t1.terminal_adress <> t2.terminal_adress
            );
        )
    '''
    cursor.execite(query)
    conn.commit()


def deleted_terminals():
    query = '''
        CREATE TABLE stg_deleted_terminals as
        FROM dwh_dim_terminals_hist t1
        LEFT JOIN stg_terminals t2
        ON t1.terminal_id = t2.terminal_id
        WHERE t2.terminal_id is NULL;
    '''
    cursor.execute(query)
    conn.commit()