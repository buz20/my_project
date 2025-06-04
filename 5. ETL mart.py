import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "1111",
    "port": "5432"
}

def transfer_data():
    """Переносит данные в mart.transfers с Status перед Transaction_status"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Очищаем таблицу перед вставкой новых данных
        cursor.execute("TRUNCATE TABLE mart.transfers")
        
        # Переносим данные с указанием порядка столбцов
        cursor.execute("""
            INSERT INTO mart.transfers 
            (Amount, Date, Country, Client_FIO, Account_Number, Status, Transaction_status)
            SELECT 
                t.Amount, 
                t.Date, 
                t.Country, 
                t.Client_FIO, 
                t.Account_Number,
                c.Status,
                CASE 
                    WHEN t.Amount > 50000 THEN 'высокий риск'
                    WHEN t.Amount BETWEEN 20000 AND 50000 THEN 
                        CASE 
                             WHEN c.Status = 'СНГ (не подозрительная)' THEN 'средний риск'
                             WHEN c.Status = 'Низкий риск' THEN 'средний риск' 
                             ELSE 'высокий риск'
                        END
                    WHEN t.Amount < 20000 THEN 
                        CASE 
                            WHEN c.Status = 'СНГ (не подозрительная)' THEN 'низкий риск' 
                            WHEN c.Status = 'Низкий риск' THEN  'низкий риск' 
                            ELSE 'высокий риск'
                        END
                END AS Transaction_status
            FROM core.transfers t
            LEFT JOIN core.countries c ON t.Country = c.Name
        """)
        
        conn.commit()
        print("Данные успешно перенесены. Status теперь предпоследний столбец.")
        
    except Exception as e:
        print(f"Ошибка при переносе данных: {e}")
        conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    transfer_data()