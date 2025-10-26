import pymongo


from pymongo import MongoClient
from pprint import pprint
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["alcomarket"]
products = db["products"]

# 2. Очистка коллекции перед загрузкой (для повторного запуска)
products.drop()

# 3. Загрузка данных из файла products.json
with open("products.json", "r") as f:
    data = json.load(f)
    products.insert_many(data)

print("\n📦 Все товары:")
for doc in products.find():
    pprint(doc)
