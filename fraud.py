# В этом файле происходит поиск мошеннических транзакций
from create_tables import create_fraud_types, create_rep_fraud
from connection import connection, cursor


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

    connection.commit()


def find_fraud(date):
    # Находит фрод и вносит записи в БД
    create_fraud_types()
    create_rep_fraud()
    update_rep_fraud(date)
