import json
from kafka import KafkaConsumer
from config import KAFKA_TOPIC, KAFKA_SERVER


def consume_articles():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="news-trend-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    articles = []
    print("Consumer reading from Kafka...")
    for message in consumer:
        articles.append(message.value)

    consumer.close()
    print(f"Consumed {len(articles)} articles!")
    return articles
