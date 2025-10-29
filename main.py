from pymongo import MongoClient
from datetime import datetime, timedelta
from pprint import pprint
import json
import os
# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
collection.drop()
# Список документов
data = [
    {
        "user_id": 123,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 10, 0, 0),
        "user_info": {
            "email": "user1@example.com",
            "registration_date": datetime(2023, 12, 1, 10, 0, 0)
        }
    },
    {
        "user_id": 124,
        "event_type": "login",
        "event_time": datetime(2024, 1, 21, 9, 30, 0),
        "user_info": {
            "email": "user2@example.com",
            "registration_date": datetime(2023, 12, 2, 12, 0, 0)
        }
    },
    {
        "user_id": 125,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 19, 14, 15, 0),
        "user_info": {
            "email": "user3@example.com",
            "registration_date": datetime(2023, 12, 3, 11, 45, 0)
        }
    },
    {
        "user_id": 126,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 16, 0, 0),
        "user_info": {
            "email": "user4@example.com",
            "registration_date": datetime(2023, 12, 4, 9, 0, 0)
        }
    },
    {
        "user_id": 127,
        "event_type": "login",
        "event_time": datetime(2024, 1, 22, 10, 0, 0),
        "user_info": {
            "email": "user5@example.com",
            "registration_date": datetime(2023, 12, 5, 10, 0, 0)
        }
    },
    {
        "user_id": 128,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 22, 11, 30, 0),
        "user_info": {
            "email": "user6@example.com",
            "registration_date": datetime(2023, 12, 6, 13, 0, 0)
        }
    },
    {
        "user_id": 129,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 23, 15, 0, 0),
        "user_info": {
            "email": "user7@example.com",
            "registration_date": datetime(2023, 12, 7, 8, 0, 0)
        }
    },
    {
        "user_id": 130,
        "event_type": "login",
        "event_time": datetime(2024, 1, 23, 16, 45, 0),
        "user_info": {
            "email": "user8@example.com",
            "registration_date": datetime(2023, 12, 8, 10, 0, 0)
        }
    },
    {
        "user_id": 131,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 24, 12, 0, 0),
        "user_info": {
            "email": "user9@example.com",
            "registration_date": datetime(2023, 12, 9, 14, 0, 0)
        }
    },
    {
        "user_id": 132,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 24, 18, 30, 0),
        "user_info": {
            "email": "user10@example.com",
            "registration_date": datetime(2023, 12, 10, 10, 0, 0)
        }
    }
]

# Заливка данных в коллекцию
collection.insert_many(data)

print("✅ Данные успешно загружены в MongoDB")

archive_data = []
for doc in collection.find({"user_info.registration_date": {"$lt":datetime.now() - timedelta(days=30)},
                            "event_time":{"$lt":datetime.now() - timedelta(days=14)}}):
    archive_data.append(doc)

collection = db["archived_users"]
collection.drop()
collection.insert_many(archive_data)

d = {}
d["date"] = datetime.now().strftime('%Y-%m-%d')
d["archived_users_count"] = 0
archived_users_ids = []

for id_ in collection.distinct("user_id"):
    archived_users_ids.append(id_)
    d["archived_users_count"] += 1
d["archived_users_ids"] = archived_users_ids

path_ = f"tmp/{datetime.now().strftime('%Y-%m-%d')}.json";

with open(path_, "w") as file:
    json.dump(d, file)
