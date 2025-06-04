import psycopg2
from psycopg2 import sql

# Настройки подключения к БД
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "1111",
    "port": "5432"
}

def clean_amount(amount_str):
    """Извлекает числовое значение из строки с RUB"""
    if not amount_str:
        return None
    # Удаляем все нечисловые символы, кроме точки и минуса
    cleaned = ''.join(c for c in amount_str if c.isdigit() or c in '.-')
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None

def transfer_countries():
    """Перенос данных о странах из raw в core"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
        INSERT INTO core.countries (id, name, status)
        SELECT id, name, status FROM raw.countries
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            status = EXCLUDED.status;
        """)
        conn.commit()
        print("Данные стран успешно перенесены в core.countries")
        
    except Exception as e:
        print(f"Ошибка при переносе стран: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

def transfer_transfers():
    """Перенос данных о транзакциях с преобразованием amount"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Получаем все данные из raw.transfers
        cursor.execute("SELECT id, amount, date, country, client_fio, account_number FROM raw.transfers")
        transfers = cursor.fetchall()
        
        # Обрабатываем и вставляем данные в core.transfers
        for row in transfers:
            try:
                amount_num = clean_amount(row[1])
                
                cursor.execute("""
                INSERT INTO core.transfers 
                (id, amount, date, country, client_fio, account_number)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    amount = EXCLUDED.amount,
                    date = EXCLUDED.date,
                    country = EXCLUDED.country,
                    client_fio = EXCLUDED.client_fio,
                    account_number = EXCLUDED.account_number;
                """, (
                    row[0],  # id
                    amount_num,  # преобразованный amount
                    row[2],  # date
                    row[3],  # country
                    row[4],  # client_fio
                    row[5]   # account_number
                ))
            except Exception as e:
                print(f"Ошибка обработки строки {row[0]}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"Успешно перенесено {len(transfers)} транзакций в core.transfers")
        
    except Exception as e:
        print(f"Ошибка при переносе транзакций: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("Начало переноса данных из raw в core...")
    transfer_countries()
    transfer_transfers()
    print("Перенос данных завершен")