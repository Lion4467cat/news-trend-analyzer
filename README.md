# 📰 Real-Time News Trend Analyzer

A streaming news intelligence pipeline that ingests, processes, and analyzes news articles in real time — detecting trends, sentiment, and source credibility as events unfold.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-2.x-000000?style=flat&logo=flask&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)
![Dask](https://img.shields.io/badge/Dask-Parallel_Computing-FDA061?style=flat)
![spaCy](https://img.shields.io/badge/spaCy-NLP-09A3D5?style=flat)

---

## 🔍 What It Does

Most news tools show you *what's trending*. This system shows you *why it's trending*, *how fast*, and *whether to trust the source* — all in real time.

| Feature | Description |
|---|---|
| **Live Ingestion** | Kafka producer streams articles from NewsAPI continuously |
| **Deduplication** | Removes duplicate stories before they pollute analysis |
| **Sentiment Analysis** | VADER scoring per article and per trend cluster |
| **Source Credibility** | Scores news sources based on reliability signals |
| **Trend Velocity** | Detects how fast a topic is accelerating |
| **Parallel Processing** | Dask handles large article volumes without blocking |
| **Web Dashboard** | Flask UI renders live trend cards and sentiment charts |

---

## 🏗️ Architecture

```
NewsAPI
   │
   ▼
Kafka Producer (kafka_producer.py)
   │  streams articles as JSON messages
   ▼
Kafka Consumer (kafka_consumer.py)
   │  deduplication → credibility scoring
   ▼
Dask Analysis (dask_analysis.py)
   │  parallel VADER sentiment + trend velocity
   ▼
Flask App (app.py)
   │  aggregates results, serves dashboard
   ▼
Web Dashboard (templates/)
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Apache Kafka running locally (or via Docker)
- NewsAPI key → [newsapi.org](https://newsapi.org)

### Installation

```bash
git clone https://github.com/Lion4467cat/news-trend-analyzer.git
cd news-trend-analyzer
pip install -r requirements.txt
```

### Configuration

Edit `config.py`:
```python
NEWS_API_KEY = "your_newsapi_key_here"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "news-stream"
```

### Run

```bash
# Terminal 1 — Start Kafka (if using Docker)
docker-compose up -d

# Terminal 2 — Start producer
python kafka_producer.py

# Terminal 3 — Start consumer + analysis
python kafka_consumer.py

# Terminal 4 — Start Flask dashboard
python app.py
```

Visit `http://localhost:5000`

---

## 🛠️ Tech Stack

- **Streaming** — Apache Kafka
- **Parallel Processing** — Dask
- **NLP** — spaCy, VADER Sentiment
- **Backend** — Flask, Python
- **Data Source** — NewsAPI

---

## 📌 Research Gaps Implemented

This project was built as a research-backed system. Features implemented beyond standard news aggregation:

- ✅ VADER sentiment with topic-level aggregation
- ✅ Source credibility scoring
- ✅ Deduplication pipeline
- ✅ Trend velocity detection
- 🔨 TF-IDF topic clustering *(in progress)*
- 🔨 Geographic heatmap *(in progress)*
- 🔨 NER disambiguation *(in progress)*

---

## 👤 Author

**S. S. Gokula Swamy** — [LinkedIn](https://www.linkedin.com/in/ssgokulaswamy) · [Portfolio](https://lion4467cat.github.io/raikabuilds)
