from dotenv import load_dotenv
import os

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")  # paste your NewsAPI key
KAFKA_TOPIC = "news-trend"
KAFKA_SERVER = "localhost:9092"
