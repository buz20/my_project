#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  1 01:30:43 2025

@author: nikolaybuzynenko
"""

import psycopg2
import csv
import os
import logging
from datetime import datetime

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Конфигурация подключения к БД
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "1111",
    "port": "5432"
}

# Путь для сохранения файла
EXPORT_PATH = "/Users/nikolaybuzynenko/Documents/Системный аналитик/DWH"
EXPORT_FILENAME = f"mart_transfers_export.csv"
FULL_PATH = os.path.join(EXPORT_PATH, EXPORT_FILENAME)

def export_mart_transfers():
    """Экспорт таблицы mart.transfers в CSV файл"""
    conn = None
    try:
        # Проверяем и создаем папку для экспорта
        os.makedirs(EXPORT_PATH, exist_ok=True)
        logger.info(f"Папка для экспорта: {EXPORT_PATH}")

        # Подключение к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("Успешное подключение к базе данных")

        # Получаем данные из таблицы mart.transfers
        cursor.execute("SELECT * FROM mart.transfers")
        rows = cursor.fetchall()
        
        # Получаем названия столбцов
        colnames = [desc[0] for desc in cursor.description]
        
        # Записываем в CSV файл
        with open(FULL_PATH, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(colnames)  # Заголовки столбцов
            writer.writerows(rows)
        
        logger.info(f"Успешно экспортировано {len(rows)} записей в файл: {FULL_PATH}")
        return True

    except Exception as e:
        logger.error(f"Ошибка при экспорте: {e}")
        return False
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с БД закрыто")

if __name__ == "__main__":
    if export_mart_transfers():
        print(f"Экспорт успешно завершен. Файл сохранен в: {FULL_PATH}")
    else:
        print("Ошибка при экспорте данных")
        sys.exit(1)