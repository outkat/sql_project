# В этом модуле происходит обновление dwh таблиц
from connection import connection, cursor
from create_tables import (
    create_fact_pb,
    create_fact_trans,
    create_stg,
    create_terminals_hist,
    create_stg_new,
    create_stg_changed,
    create_stg_deleted
)
from data_extraction import load_data_from_files


def join_stg_new():
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
    connection.commit()


def join_stg_changed():
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
    connection.commit()


def join_stg_deleted():
    # Обновление удаленных данных
    query = '''
        UPDATE dwh_dim_terminals_hist
        SET deleted_flg = 1,
            effective_to = current_timestamp
        WHERE terminal_id IN (SELECT terminal_id FROM stg_terminals_deleted);
    '''
    cursor.execute(query)
    connection.commit()


def update_dwh(path, date):
    # Загрузка файлов в исторические таблицы
    # path - путь до файлов
    # date - дата на которую нужно загрузить
    create_stg()
    load_data_from_files(path, date)

    create_fact_pb()
    create_fact_trans()
    create_terminals_hist()

    for stg_table, scd2_table, on in zip(
        ['stg_transactions', 'stg_passport_blacklist', 'stg_terminals'],
        ['dwh_fact_transactions', 'dwh_fact_passport_blacklist', 'dwh_dim_terminals_hist'],
        ['trans_id', 'passport_num', 'terminal_id']
    ):
        create_stg_new(stg_table, scd2_table, on)

    create_stg_changed()
    create_stg_deleted()

    join_stg_new()
    join_stg_changed()
    join_stg_deleted()

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

    connection.commit()
