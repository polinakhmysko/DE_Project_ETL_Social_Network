import json
import random
from datetime import datetime, timedelta
from faker import Faker
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

fake = Faker()

# Генерация данных пользователей

def generate_users():
    return [{
        "user_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "registered_at": fake.date_time_this_year().isoformat(),
        "age": random.randint(18, 70),
        "gender": random.choice(["male", "female"]),
        "device_type": random.choice(["computer", "mobile", "tablet"]),
        "os": random.choice(["Windows", "macOS", "Android", "iOS"]),
        "ip_address": fake.ipv4(),
        "country": fake.country()
    } for _ in range(10)]

# Генерация данных активности пользователей

def generate_sessions(users):
    sessions = []
    for u in users:
        for _ in range(random.randint(1, 3)):
            start = fake.date_time_this_month()
            sessions.append({
                "session_id": fake.uuid4(),
                "user_id": u["user_id"],
                "start_time": start.isoformat(),
                "end_time": (start + timedelta(minutes=random.randint(5, 45))).isoformat(),
                "pages_viewed": random.randint(1, 12)
            })
    return sessions

def generate_events(sessions):
    return [{
        "event_id": fake.uuid4(),
        "session_id": s["session_id"],
        "user_id": s["user_id"],
        "event_type": random.choice(["view", "add_to_cart", "purchase"]),
        "timestamp": s["start_time"]
    } for s in sessions for _ in range(random.randint(1, 4))]

# Генерация данных по заказам пользователей

def generate_orders(users):
    categories = ["electronics", "books", "clothes", "toys"]
    product_types = {
        "electronics": ["Smartphone", "Laptop", "Headphones", "Camera", "Tablet"],
        "books": ["Fantasy Novel", "Biography", "Cookbook", "Textbook", "Science Fiction"],
        "clothes": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress"],
        "toys": ["Robot", "Puzzle", "Board Game", "Racing Car", "Doll"]
    }
    
    orders = []
    for _ in range(random.randint(1, 4)):
        category = random.choice(categories)
        product = random.choice(product_types[category])
        quantity = random.randint(1, 4)
        price = round(random.uniform(50, 1000), 2)
        total_price = round(quantity * price, 2)

        orders.append ({
            "order_id": fake.uuid4(),
            "user_id": random.choice(users)["user_id"],
            "created_at": fake.date_time_this_month().isoformat(),
            "category": category,
            "product": product,
            "quantity": quantity,
            "price": price,
            "total_price": total_price,
            "payment_method": random.choice(["card", "e_wallet"]),
            "supplier": fake.company(),
            "supplier_country": fake.country()
        })
    return orders

# Генерация данных по маркетинговым кампаниям,
# пользователям, участвующим в данных кампаниях

def generate_campaigns():
    return [{
        "campaign_id": fake.uuid4(),
        "name": f"Campaign {i}",
        "start_date": fake.date_this_month().isoformat(),
        "end_date": (fake.date_this_month() + timedelta(days=10)).isoformat(),
        "promocode": fake.lexify(text='PROMO????'),
        "discount_percent": random.choice([5, 10, 15, 20])
    } for i in range(3)]

def generate_user_campaigns(users, campaigns):
    return [{
        "user_id": u["user_id"],
        "campaign_id": random.choice(campaigns)["campaign_id"]
    } for u in users if random.random() < 0.5]

# Загрузка сгенерированных данных в Minio

def upload_json_to_minio(data, name):
    hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'raw-data-bucket'

    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name)

    filename = f"{name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    body = json.dumps(data, ensure_ascii=False, indent=2)

    hook.load_string(
        string_data=body,
        key=f'/{name}/' + filename,
        bucket_name=bucket_name,
        replace=False,
        encoding='utf-8'
    )

    logging.info(f'Файл {filename} загружен в Minio')

# Главная функция по генерации всех данных

def generate_all_data():
    users = generate_users()
    sessions = generate_sessions(users)
    events = generate_events(sessions)
    orders = generate_orders(users)
    campaigns = generate_campaigns()
    user_campaigns = generate_user_campaigns(users, campaigns)

    data = {
        "users": users,
        "sessions": sessions,
        "events": events,
        "orders": orders,
        "campaigns": campaigns,
        "userCampaigns": user_campaigns
    }

    for name, items in data.items():
        upload_json_to_minio(items, name)

    logging.info('Данные успешно сгенерированы')