import json
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config

INPUT_TOPIC = 'oleg_building_sensors'
TEMP_ALERT_TOPIC = 'oleg_temperature_alerts'
HUMIDITY_ALERT_TOPIC = 'oleg_humidity_alerts'

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    data = message.value

    if data["temperature"] > 40:
        alert = {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "temperature": data["temperature"],
            "message": "Temperature threshold exceeded"
        }
        producer.send(TEMP_ALERT_TOPIC, value=alert)
        producer.flush()
        print(f"[Temperature Alert] {alert}")

    if data["humidity"] < 20 or data["humidity"] > 80:
        alert = {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "humidity": data["humidity"],
            "message": "Humidity threshold exceeded"
        }
        producer.send(HUMIDITY_ALERT_TOPIC, value=alert)
        producer.flush()
        print(f"[Humidity Alert] {alert}")
