import csv
import random
from datetime import datetime, timedelta
from faker import Faker

# Инициализация Faker для русских данных
fake = Faker('ru_RU')

# Настройки генерации транзакций
RECORD_COUNT = 10000
MIN_AMOUNT = 1000
MAX_AMOUNT = 300000
CLIENT_POOL_SIZE = 100

# Определение списков стран и их статусов
def setup_countries():
    # Список стран СНГ (изначально не подозрительные, но некоторые могут попасть под критерии)
    cng_countries = {
        "Россия", "Казахстан", "Беларусь", "Армения",
        "Киргизия", "Азербайджан", "Узбекистан", "Таджикистан"
    }

    # Черный список FATF (высокий риск)
    fatf_blacklist = {"Иран", "КНДР", "Мьянма"}

    # Серый список FATF (усиленный мониторинг)
    fatf_greylist = {
        "Албания", "Барбадос", "Буркина-Фасо", "Камбоджа", 
        "Каймановы острова", "Гана", "Гаити", "Ямайка", 
        "Иордания", "Мали", "Мозамбик", "Нигерия", 
        "Панама", "Филиппины", "Сенегал", "ЮАР", 
        "Южный Судан", "Сирия", "Танзания", "Турция", 
        "Уганда", "ОАЭ", "Йемен", "Зимбабве"
    }

    # Санкционные списки (OFAC, ЕС)
    sanctioned_countries = {
       "Иран", "Сирия", "Куба", 
        "КНДР", "Венесуэла", "Зимбабве", "Судан", 
        "Афганистан", "Мьянма", "Никарагуа"
    }

    # Офшорные зоны (высокий риск)
    offshore_zones = {
        "Белиз", "Бермуды", "Британские Виргинские острова", 
        "Каймановы острова", "Кипр", "Джерси", "Лихтенштейн", 
        "Маврикий", "Мальта", "Монако", "Панама", "Сейшелы", 
        "Сингапур", "Швейцария"
    }

    # Страны с высоким уровнем коррупции (Transparency International)
    high_corruption_countries = {
        "Афганистан", "Ангола", "Венесуэла", "Гаити", 
        "Ирак", "Йемен", "Ливия", "Нигерия", 
        "Сомали", "Южный Судан", "Сирия", "Зимбабве"
    }

    # Базовый список стран (первые 8 - СНГ)
    countries = [
        (1, "Россия"), (2, "Казахстан"), (3, "Беларусь"), 
        (4, "Армения"), (5, "Киргизия"), (6, "Азербайджан"),
        (7, "Узбекистан"), (8, "Таджикистан")
    ]

    # Дополнительные страны (включая США, Китай, Германию, Францию, Турцию, ОАЭ и др.)
    additional_countries = [
        (9, "Афганистан"), (10, "Албания"), (11, "Алжир"),
        (12, "Ангола"), (13, "Аргентина"), (14, "Австралия"),
        (15, "Австрия"), (16, "Бахрейн"), (17, "Бангладеш"),
        (18, "Бельгия"), (19, "Бенин"), (20, "Боливия"),
        (21, "Ботсвана"), (22, "Бразилия"), (23, "Болгария"),
        (24, "Буркина-Фасо"), (25, "Бурунди"), (26, "Вьетнам"),
        (27, "Габон"), (28, "Гаити"), (29, "Гамбия"),
        (30, "Гана"), (31, "Гватемала"), (32, "Гвинея"),
        (33, "Германия"), (34, "Гондурас"), (35, "Греция"),
        (36, "Грузия"), (37, "Дания"), (38, "Джибути"),
        (39, "Египет"), (40, "Замбия"), (41, "Зимбабве"),
        (42, "Израиль"), (43, "Индия"), (44, "Индонезия"),
        (45, "Иран"), (46, "Ирак"), (47, "Ирландия"),
        (48, "Исландия"), (49, "Испания"), (50, "Италия"),
        (51, "Йемен"), (52, "Камерун"), (53, "Канада"),
        (54, "Катар"), (55, "Кения"), (56, "Кипр"),
        (57, "Китай"), (58, "Колумбия"), (59, "Куба"),
        (60, "Кувейт"), (61, "Лаос"), (62, "Латвия"),
        (63, "Ливан"), (64, "Ливия"), (65, "Литва"),
        (66, "Люксембург"), (67, "Маврикий"), (68, "Малайзия"),
        (69, "Мали"), (70, "Мальта"), (71, "Марокко"),
        (72, "Мексика"), (73, "Молдова"), (74, "Монако"),
        (75, "Монголия"), (76, "Мьянма"), (77, "Непал"),
        (78, "Нигерия"), (79, "Нидерланды"), (80, "Никарагуа"),
        (81, "Новая Зеландия"), (82, "Норвегия"), (83, "ОАЭ"),
        (84, "Оман"), (85, "Пакистан"), (86, "Панама"),
        (87, "Перу"), (88, "Польша"), (89, "Португалия"),
        (90, "Руанда"), (91, "Румыния"), (92, "США"),
        (93, "Саудовская Аравия"), (94, "Сенегал"), (95, "Сербия"),
        (96, "Сингапур"), (97, "Сирия"), (98, "Словакия"),
        (99, "Словения"), (100, "Сомали"), (101, "Судан"),
        (102, "Таиланд"), (103, "Танзания"), (104, "Тунис"),
        (105, "Турция"), (106, "Уганда"), 
        (108, "Уругвай"), (109, "Филиппины"), (110, "Финляндия"),
        (111, "Франция"), (112, "Хорватия"), (113, "Чад"),
        (114, "Черногория"), (115, "Чехия"), (116, "Чили"),
        (117, "Швейцария"), (118, "Швеция"), (119, "Шри-Ланка"),
        (120, "Эквадор"), (121, "Эстония"), (122, "Эфиопия"),
        (123, "ЮАР"), (124, "Южная Корея"), (125, "Япония")
    ]

    # Добавляем дополнительные страны
    countries.extend(additional_countries)
    
    # Функция определения статуса страны
    def get_country_status(name):
        if name in fatf_blacklist:
            return "Черный список FATF (высокий риск)"
        elif name in sanctioned_countries:
            return "Санкционная (высокий риск)"
        elif name in fatf_greylist:
            return "Серый список FATF (риск)"
        elif name in offshore_zones:
            return "Офшор (риск)"
        elif name in high_corruption_countries:
            return "Высокая коррупция (риск)"
        elif name in cng_countries:
            return "СНГ (не подозрительная)"
        else:
            return "Низкий риск"
    
    # Создаем данные для таблицы стран
    table_data = []
    for id, name in countries:
        status = get_country_status(name)
        table_data.append({
            "ID": id,
            "Name": name,
            "Status": status
        })
    
    return table_data, [name for _, name in countries]

# Генерация данных транзакций
def generate_transactions(country_names):
    # Создадим пул клиентов (100 уникальных клиентов)
    clients = []
    for _ in range(CLIENT_POOL_SIZE):
        full_name = fake.last_name() + ' ' + fake.first_name() + ' ' + fake.middle_name()
        account_number = '40702' + ''.join([str(random.randint(0, 9)) for _ in range(13)])  # Формат 40702810XXXXXXXXX
        clients.append({
            "full_name": full_name,
            "account_number": account_number
        })

    # Генерация случайных дат за последний год
    def random_timestamp():
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        random_seconds = random.randint(0, 365*24*60*60)
        return (start_date + timedelta(seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")

    # Генерация данных
    transactions = []
    for i in range(1, RECORD_COUNT + 1):
        amount_rub = f"{round(random.uniform(MIN_AMOUNT, MAX_AMOUNT), 2)} RUB"  # Сумма с валютой
        client = random.choice(clients)  # Выбираем случайного клиента из пула
        
        transactions.append({
            "id": i,
            "Amount": amount_rub,  # Формат "123456.78 RUB"
            "Date": random_timestamp(),
            "Country": random.choice(country_names),
            "Client_FIO": client["full_name"],
            "Account_Number": client["account_number"]
        })
    
    return transactions

# Основная функция
def main():
    # Создаем данные о странах
    countries_data, country_names = setup_countries()
    
    # Сохраняем страны в CSV
    countries_filename = "countries_with_status.csv"
    with open(countries_filename, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["ID", "Name", "Status"])
        writer.writeheader()
        writer.writerows(countries_data)
    
    print(f"Файл {countries_filename} успешно создан. Всего стран: {len(countries_data)}")
    print("\nСтраны с высоким риском:")
    for item in countries_data:
        if "риск" in item['Status'].lower():
            print(f"- {item['Name']} ({item['Status']})")
    
    # Генерируем транзакции
    transactions = generate_transactions(country_names)
    
    # Сохраняем транзакции в CSV
    transactions_filename = "transactions_rub_combined_with_clients.csv"
    with open(transactions_filename, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["id", "Amount", "Date", "Country", 
                                                "Client_FIO", "Account_Number"])
        writer.writeheader()
        for row in transactions:
            writer.writerow(row)
    
    print(f"\nФайл {transactions_filename} успешно создан с {RECORD_COUNT} транзакциями")
    print(f"Диапазон сумм: от {MIN_AMOUNT} RUB до {MAX_AMOUNT} RUB")
    print(f"Уникальных клиентов: {CLIENT_POOL_SIZE}")

if __name__ == "__main__":
    main()