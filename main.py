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
            dst_name = f'archive/{table}_{date}.txt'
        elif format == 'xlsx':
            file_name = f'{path}/{table}_{date}.xlsx'
            dst_name = f'archive/{table}_{date}.xlsx'

        file2sql(file_name, f'stg_{table}', 'myproject', format)
        os.rename(file_name, dst_name + '.backup')


def init_stg():
    # Инициализация стейджинговых таблиц
    query = '''
    CREATE TABLE if not exists stg_passport_blacklist(
        passport_num varchar(128),
        entry_dt date
    );

    CREATE TABLE if not exists stg_transactions(
        trans_id varchar(128),
        trans_date timestamp,
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
        trans_date timestamp,
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
                AND t1.terminal_address <> t2.terminal_address
                AND t2.deleted_flg = 0
    '''
    cursor.execute(query)
    conn.commit()


def create_stg_deleted():
    # Логическое удаление терминалов
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
    # Внести изменения из stg таблиц с новыми записями
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
        UPDATE dwh_dim_terminals_hist t1
        SET effective_to = current_timestamp - interval '1' second,
        deleted_flg = 1
        FROM stg_terminals_changed t2
        WHERE t1.terminal_id = t2.terminal_id and 
        (t1.terminal_city <> t2.terminal_city or
        t1.terminal_address <> t2.terminal_address)
    '''
    cursor.execute(query)
    query = '''
    INSERT INTO dwh_dim_terminals_hist(
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
    ) SELECT terminal_id,
        terminal_type,
        terminal_city,
        terminal_address
    FROM stg_terminals_changed
    '''
    cursor.execute(query)
    conn.commit()


def merge_stg_deleted():
    # Обновление удаленных данных
    query = '''
        UPDATE dwh_dim_terminals_hist
        SET deleted_flg = 1,
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
        DROP TABLE if exists {table};
        """
        cursor.execute(query)

    conn.commit()


def init_fraud_types():
    # Инициализация таблицы с расшифровкой типов фрода
    query = '''
        CREATE TABLE IF NOT EXISTS fraud_types(
        fraud_id serial primary key,
        name varchar(128),
        description text
        )
    '''
    cursor.execute(query)
    query = '''
        INSERT INTO fraud_types(
        fraud_id,
        name, 
        description
        )
        VALUES 
        (1, 'invalid_passport', 'Совершение операции при просроченном или заблокированном паспорте'),
        (2, 'invalid_contract', 'Совершение операции при недействующем договоре'),
        (3, 'different_cities', 'Совершение операций в разных городах в течение одного часа'),
        (4, 'selection_of_amount', 'Попытка подбора суммы')
    '''
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()


def init_rep_fraud():
    # Инцицализация отчета по фроду
    query = '''
        CREATE TABLE if not exists rep_fraud(
        event_dt timestamp,
        passport varchar(128),
        fio varchar(128),
        phone varchar(128),
        event_type varchar(128),
        report_dt timestamp default current_timestamp);
    '''
    cursor.execute(query)
    conn.commit()


def update_rep_fraud(date):
    date = f'{date[0:2]}-{date[2:4]}-{date[4:]}'
    # Заблокированный или просроченный паспорт
    query = f'''
    INSERT INTO rep_fraud(
        event_dt,
        passport,
        fio,
        phone,
        event_type,
        report_dt
    )
    SELECT
        t1.trans_date event_dt,
        t4.passport_num,
        concat(t4.first_name, ' ', t4.last_name) fio,
        t4.phone,
        1 event_type,
        current_timestamp
    FROM dwh_fact_transactions t1
        INNER JOIN cards t2
            ON t1.card_num = t2.card_num
        INNER JOIN accounts t3
            ON t2.account = t3.account
        INNER JOIN clients t4
            ON t3.client = t4.client_id
        LEFT JOIN dwh_fact_passport_blacklist t5
            on t4.passport_num = t5.passport_num
    WHERE
        t1.trans_date > t4.passport_valid_to
        or t5.passport_num is not null
        and date(t1.trans_date) = '{date}'
    '''
    cursor.execute(query)

    # Истек срок договора
    query = f'''
    INSERT INTO rep_fraud(
        event_dt,
        passport,
        fio,
        phone,
        event_type,
        report_dt
    )
    SELECT
        t1.trans_date event_dt,
        t4.passport_num,
        concat(t4.first_name, ' ', t4.last_name) fio,
        t4.phone,
        2 event_type,
        current_timestamp
    FROM dwh_fact_transactions t1
        INNER JOIN cards t2
            ON t1.card_num = t2.card_num
        INNER JOIN accounts t3
            ON t2.account = t3.account
        INNER JOIN clients t4
            ON t3.client = t4.client_id
    WHERE
        t3.valid_to < t1.trans_date
        AND date(t1.trans_date) = '{date}'
    '''
    cursor.execute(query)

    # Операции в разных городах
    query = f'''
    with q1 as (
        SELECT
            t1.trans_date event_dt,
            LAG(t1.trans_date) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS event_dt_lag,
            t4.passport_num,
            CONCAT(t4.first_name, ' ', t4.last_name) fio,
            t4.phone,
            3 event_type,
            current_timestamp report_dt,
            t5.terminal_city,
            LAG(t5.terminal_city) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS terminal_city_lag
        FROM dwh_fact_transactions t1
            INNER JOIN cards t2
                ON t1.card_num = t2.card_num
            INNER JOIN accounts t3
                ON t2.account = t3.account
            INNER JOIN clients t4
                ON t3.client = t4.client_id
            INNER JOIN dwh_dim_terminals_hist t5
                ON t1.terminal = t5.terminal_id
                    AND t1.create_dt < t5.effective_to
                    AND t1.create_dt >= t5.effective_from
    )

    INSERT INTO rep_fraud(
        event_dt,
        passport,
        fio,
        phone,
        event_type,
        report_dt
    )
    SELECT
        event_dt,
        passport_num,
        fio,
        phone,
        event_type,
        report_dt
    FROM q1
    WHERE (event_dt - event_dt_lag) < interval '1' hour
        AND terminal_city != terminal_city_lag
        AND date(event_dt) = '{date}'
    '''
    cursor.execute(query)

    # Подбор суммы
    query = f'''
    with q1 as (
        SELECT
            t1.trans_date event_dt,
            t1.amt,
            LAG(t1.amt, 1) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS amt_lag_1,
            LAG(t1.amt, 2) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS amt_lag_2,
            LAG(t1.amt, 3) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS amt_lag_3,
            LAG(t1.trans_date, 3) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS event_dt_lag_3,
            t1.oper_result,
            LAG(t1.oper_result, 1) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS oper_result_lag_1,
            LAG(t1.oper_result, 2) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS oper_result_lag_2,
            LAG(t1.oper_result, 3) OVER (PARTITION BY t1.card_num ORDER BY t1.trans_date)
                AS oper_result_lag_3,
            t4.passport_num,
            CONCAT(t4.first_name, ' ', t4.last_name) fio,
            t4.phone,
            4 event_type,
            current_timestamp report_dt
            
        FROM dwh_fact_transactions t1
            INNER JOIN cards t2
                ON t1.card_num = t2.card_num
            INNER JOIN accounts t3
                ON t2.account = t3.account
            INNER JOIN clients t4
                ON t3.client = t4.client_id
    )

    INSERT INTO rep_fraud(
        event_dt,
        passport,
        fio,
        phone,
        event_type,
        report_dt
    )
    SELECT
        event_dt,
        passport_num,
        fio,
        phone,
        event_type,
        report_dt
    FROM q1
    WHERE (event_dt - event_dt_lag_3) < interval '20' minute
        AND amt_lag_3 IS NOT NULL
        AND amt < amt_lag_1
        AND amt_lag_1 < amt_lag_2
        AND amt_lag_2 < amt_lag_3
        AND date(event_dt) = '{date}'
        AND oper_result = 'SUCCESS'
        AND oper_result_lag_1 = 'REJECT'
        AND oper_result_lag_2 = 'REJECT'
        AND oper_result_lag_3 = 'REJECT'
    '''
    cursor.execute(query)

    conn.commit()


def find_fraud(date):
    # Находит фрод и вносит записи в БД
    init_fraud_types()
    init_rep_fraud()
    update_rep_fraud(date)


def main(date):
    etl('data', date)
    find_fraud(date)


if __name__ == '__main__':
    for i in range(1, 4):
        main(f'0{i}032021')
