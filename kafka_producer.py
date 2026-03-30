import requests
import json
from kafka import KafkaProducer
from config import NEWS_API_KEY, KAFKA_TOPIC, KAFKA_SERVER

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def fetch_and_send(from_date, to_date, query="world"):
    print(f"Fetching news from {from_date} to {to_date}...")
    url = (
        f"https://newsapi.org/v2/everything?"
        f"q={query}&"
        f"from={from_date}&"
        f"to={to_date}&"
        f"language=en&"
        f"sortBy=publishedAt&"
        f"pageSize=100&"
        f"apiKey={NEWS_API_KEY}"
    )
    response = requests.get(url)
    data = response.json()

    if data.get("status") != "ok":
        print(f"API Error: {data.get('message')}")
        return 0

    articles = data.get("articles", [])
    print(f"Found {len(articles)} articles. Sending to Kafka...")

    for article in articles:
        message = {
            "title": article.get("title", "") or "",
            "description": article.get("description", "") or "",
            "content": article.get("content", "") or "",
            "source": article.get("source", {}).get("name", "") or "",
            "publishedAt": article.get("publishedAt", "") or "",
            "url": article.get("url", "") or "",
        }
        producer.send(KAFKA_TOPIC, value=message)

    producer.flush()
    print(f"Sent {len(articles)} articles to Kafka!")
    return len(articles)
