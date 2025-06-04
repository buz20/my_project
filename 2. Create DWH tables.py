"""
Created on Thu May 22 21:51:07 2025

@author: nikolaybuzynenko
"""

import psycopg2
from psycopg2 import sql

# Параметры подключения к PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "1111",
    "port": "5432"
}

# SQL-запросы для создания слоёв
SQL_SCRIPTS = {
    "create_schemas": [
        "CREATE SCHEMA IF NOT EXISTS raw;",
        "CREATE SCHEMA IF NOT EXISTS core;",
        "CREATE SCHEMA IF NOT EXISTS mart;"
    ],
    "create_raw_tables": [
        """
        CREATE TABLE IF NOT EXISTS raw.transfers (
            id SERIAL PRIMARY KEY,
            Amount VARCHAR(250) NOT NULL,
            Date TIMESTAMP,
            Country VARCHAR(50) NOT NULL,
            Client_FIO VARCHAR(250) NOT NULL,
            Account_Number VARCHAR(250) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.countries (
            ID SERIAL PRIMARY KEY,
            Name VARCHAR(50) NOT NULL UNIQUE,
            Status VARCHAR(50) NOT NULL
        )
        """
    ],
    "create_core_tables": [
        """
        CREATE TABLE IF NOT EXISTS core.transfers (
            id SERIAL PRIMARY KEY,
            Amount DECIMAL(16, 2) NOT NULL,
            Date TIMESTAMP,
            Country VARCHAR(50) NOT NULL,
            Client_FIO VARCHAR(250) NOT NULL,
            Account_Number VARCHAR(250) NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS core.countries (
            ID SERIAL PRIMARY KEY,
            Name VARCHAR(50) NOT NULL UNIQUE,
            Status VARCHAR(50) NOT NULL
        )
        """
    ],
    "create_mart_views": [
        """
        CREATE TABLE IF NOT EXISTS mart.transfers (
            id SERIAL PRIMARY KEY,
            Amount DECIMAL(16, 2) NOT NULL,
            Date TIMESTAMP,
            Country VARCHAR(50) NOT NULL,
            Client_FIO VARCHAR(250) NOT NULL,
            Account_Number VARCHAR(250) NOT NULL,
            Status VARCHAR(50) NOT NULL,
            Transaction_status VARCHAR(50) NOT NULL
        )
        """
    ]
}

def create_layers():
    """Создает слои Raw, Core, Mart и заполняет их тестовыми таблицами."""
    conn = None
    try:
        # Подключение к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Создание схем
        print("Создание схем Raw, Core, Mart")
        for query in SQL_SCRIPTS["create_schemas"]:
            cursor.execute(query)

        # 2. Создание таблиц в Raw
        print("Создание таблиц в Raw...")
        for query in SQL_SCRIPTS["create_raw_tables"]:
            cursor.execute(query)

        # 3. Создание таблиц в Core
        print("Создание таблиц в Core...")
        for query in SQL_SCRIPTS["create_core_tables"]:
            cursor.execute(query)

        # 4. Создание витрин в Mart
        print("Создание витрин в Mart...")
        for query in SQL_SCRIPTS["create_mart_views"]:
            cursor.execute(query)

        # Фиксация изменений
        conn.commit()
        print("Слои успешно созданы!")

    except Exception as e:
        print(f"Ошибка: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_layers()