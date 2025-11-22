1. запускаем producer: python producer.py
    Создается и заполняется таблица источника на pg.
    Достаем запись, не отправленную в kafka 
    ( sent_to_kafka = FALSE ), 
    отправляем в kafka, и помечаем ее как отправленную
    ( sent_to_kafka = TRUE ).
    Цикл продолжается пока есть хоть одна запись соот-я 
    условию sent_to_kafka = FALSE.
2. запускаем consumer: python consumer.py
    Записи, полученные от roducer, приходят в ClickHouse 
    без дубликатов.
    