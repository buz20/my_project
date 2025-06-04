import psycopg2
import csv
from psycopg2 import sql
from datetime import datetime

# Настройки подключения к БД
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "1111",
    "port": "5432"
}

def load_countries_csv():
    """Загружает CSV файл со странами в сырой слой БД"""
    csv_file_path = "countries_with_status.csv"
    schema_name = "raw" 
    table_name = "countries"
    
    conn = None
    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Загружаем данные из CSV
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            total_rows = 0
            success_rows = 0
            
            for row in reader:
                total_rows += 1
                try:
                    # Проверяем наличие обязательных полей
                    if not all(field in row for field in ['ID', 'Name', 'Status']):
                        raise ValueError("Отсутствуют обязательные поля в CSV")
                    
                    # Используем INSERT ON CONFLICT для обновления существующих записей
                    insert_query = sql.SQL("""
                    INSERT INTO {}.{} (id, name, status)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        status = EXCLUDED.status;
                    """).format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name))
                    
                    cursor.execute(insert_query, (
                        int(row['ID']),
                        row['Name'],
                        row['Status']
                    ))
                    success_rows += 1
                except (ValueError, KeyError) as e:
                    print(f"Ошибка данных в строке {total_rows}: {e}")
                    continue
                except Exception as e:
                    print(f"Ошибка БД в строке {total_rows}: {e}")
                    conn.rollback()
                    continue
        
        conn.commit()
        print(f"Успешно обработано {success_rows}/{total_rows} строк в таблицу {schema_name}.{table_name}")
        
    except Exception as e:
        print(f"Критическая ошибка при загрузке стран: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def load_transfers_csv():
    """Загружает CSV файл с транзакциями в сырой слой БД"""
    csv_file = "transactions_rub_combined_with_clients.csv"
    table_name = "raw.transfers"
    
    conn = None
    try:
        # Подключение к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Загрузка данных из CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_rows = 0
            success_rows = 0
            
            for row in reader:
                total_rows += 1
                try:
                    # Проверяем наличие обязательных полей
                    if not all(field in row for field in ['id', 'Amount', 'Date', 'Country', 'Client_FIO', 'Account_Number']):
                        raise ValueError("Отсутствуют обязательные поля в CSV")
                    
                    # Используем INSERT ON CONFLICT для обновления существующих записей
                    cursor.execute(
                        f"""
                        INSERT INTO {table_name} 
                        (id, Amount, Date, Country, Client_FIO, Account_Number)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            Amount = EXCLUDED.Amount,
                            Date = EXCLUDED.Date,
                            Country = EXCLUDED.Country,
                            Client_FIO = EXCLUDED.Client_FIO,
                            Account_Number = EXCLUDED.Account_Number
                        """,
                        (
                            row['id'], 
                            row['Amount'],
                            row['Date'], 
                            row['Country'],
                            row['Client_FIO'],  
                            row['Account_Number']  
                        )
                    )
                    success_rows += 1
                except (ValueError, KeyError) as e:
                    print(f"Ошибка данных в строке {total_rows}: {e}")
                    continue
                except Exception as e:
                    print(f"Ошибка БД в строке {total_rows}: {e}")
                    conn.rollback()
                    continue
        
        conn.commit()
        print(f"Успешно обработано {success_rows}/{total_rows} строк в таблицу {table_name}")
        
    except Exception as e:
        print(f"Критическая ошибка при загрузке транзакций: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("Начало загрузки данных...")
    load_countries_csv()
    load_transfers_csv()
    print("Загрузка данных завершена")