
import json
from kafka import KafkaConsumer
from configs import kafka_config

TOPICS = ['oleg_temperature_alerts', 'oleg_humidity_alerts']

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

for message in consumer:
    alert = message.value
    print(f"[{message.topic}] ALERT: {alert}")
