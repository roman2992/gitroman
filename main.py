import pymongo


from pymongo import MongoClient
from pprint import pprint
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["alcomarket"]
products = db["products"]


