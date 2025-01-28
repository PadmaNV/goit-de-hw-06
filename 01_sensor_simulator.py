from kafka import KafkaProducer
import json
import time
import random
import string
from configs import kafka_config

TOPIC_NAME = "VN_building_sensors"

# Генеруємо унікальний sensor_id при запуску
sensor_id = ''.join(random.choices(string.digits +string.ascii_lowercase + string.digits, k=5))

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting sensor data simulation with sensor_id: {sensor_id}")
try:
    while True:
        sensor_data = {
            "sensor_id": sensor_id,
            "timestamp": int(time.time()),
            "temperature": random.randint(30, 50),
            "humidity": random.randint(0, 100)
        }
        producer.send(TOPIC_NAME, value=sensor_data)
        print(f"Sent: {sensor_data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping simulation.")
    producer.close()
