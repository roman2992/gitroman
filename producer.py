# producer_pg_to_kafka.py
import datetime

import psycopg2
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

# создание и заполнение таблицы источника на pg
cursor.execute("""
DROP TABLE IF EXISTS user_logins;
CREATE TABLE user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka boolean default False
);
""")
users = ["alice", "bob", "carol", "dave"]

i = 0
while i < 100:
    sql = f""" insert into user_logins (username, event_type, event_time) 
        select '{random.choice(users)}', 'login', to_timestamp('{time.time()}') """
    cursor.execute(sql)
    time.sleep(0.5)
    i += 1
conn.commit()

cursor.execute("SELECT username, event_type, extract(epoch FROM event_time), sent_to_kafka, event_time \
               FROM user_logins where sent_to_kafka = FALSE limit 1;")
row = cursor.fetchone()

while row:
    data = {
        "user": row[0],
        "event": row[1],
        "timestamp": float(row[2])  # преобразуем Decimal → float
    }
    producer.send("user_events", value=data)
    print("Sent:", data)
    time.sleep(0.5)

    cursor.execute(
        f"""update user_logins
         set sent_to_kafka = TRUE
         where event_type = '{row[1]}'
         and event_time = '{row[4]}'
         and username = '{row[0]}'
         ;""")
    conn.commit()
    cursor.execute(
       "SELECT username, event_type, extract(epoch FROM event_time),sent_to_kafka,event_time FROM \
       user_logins where sent_to_kafka = FALSE \
       limit 1;")

    row = cursor.fetchone()
cursor.close()
conn.close()
