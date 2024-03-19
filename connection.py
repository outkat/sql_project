# В этом модуле происходит присоединение к postgres
import psycopg2

config = {
    "host": "localhost",
    "database": "sbx",
    "user": "postgres",
    "password": "postgres",
    "port": "5432",
    "options": "-c search_path=myproject"
}

connection = psycopg2.connect(**config)
cursor = connection.cursor()

cursor.execute("create schema if not exists myproject")
connection.commit()
