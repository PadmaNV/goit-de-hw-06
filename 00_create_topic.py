from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from configs import kafka_config
import time

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Ідентифікатор для топіків
identifier = "VN"

# Визначення топіків
topics = [
    NewTopic(name=f"{identifier}_building_sensors", num_partitions=2, replication_factor=1),
    NewTopic(name=f"{identifier}_avg", num_partitions=1, replication_factor=1),
    NewTopic(name=f"{identifier}_avg_alerts", num_partitions=1, replication_factor=1)
]

# Видалення топіків, якщо вони існують
try:
    existing_topics = admin_client.list_topics()
    topics_to_delete = [topic.name for topic in topics if topic.name in existing_topics]
    if topics_to_delete:
        admin_client.delete_topics(topics_to_delete)
        print(f"Topics {[topic for topic in topics_to_delete]} deleted successfully.")
        time.sleep(5)  # Додаємо затримку для синхронізації
except Exception as e:
    print(f"An error occurred during topic deletion: {e}")

# Створення топіків
try:
    topics_to_create = [topic for topic in topics if topic.name not in admin_client.list_topics()]
    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"Topics {[topic.name for topic in topics_to_create]} created successfully.")
    else:
        print("All topics already exist.")
except Exception as e:
    print(f"An error occurred during topic creation: {e}")

# Виведення всіх топіків із префіксом VN
all_topics = admin_client.list_topics()
my_topics = [topic for topic in all_topics if topic.startswith(identifier)]
print("My topics:", my_topics)

# Закриття зв'язку з клієнтом
admin_client.close()
