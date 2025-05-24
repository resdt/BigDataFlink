import json
import os

import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

DATA_DIR = "исходные данные"

id = 1
for filename in os.listdir(DATA_DIR):
    if filename.endswith(".csv"):
        print(f"📤 Отправка файла: {filename}")
        df = pd.read_csv(os.path.join(DATA_DIR, filename))
        for _, row in df.iterrows():
            data = row.to_dict()
            data["id"] = id
            id += 1
            try:
                producer.send("mock-data", value=data)
            except Exception as e:
                print(f"Ошибка при отправке: {e}")

producer.flush()
print("✅ Все данные отправлены в Kafka.")
