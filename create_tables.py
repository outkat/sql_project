# В этом модуле описаны функции создания всех таблиц
from connection import connection, cursor


def create_stg():
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
    connection.commit()


def create_fact_pb():
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
    connection.commit()


def create_fact_trans():
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
    connection.commit()


def create_terminals_hist():
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
    connection.commit()


def create_fraud_types():
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
        connection.commit()
    except Exception as e:
        connection.rollback()


def create_rep_fraud():
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
    connection.commit()


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
    connection.commit()


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
    connection.commit()


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
    connection.commit()
