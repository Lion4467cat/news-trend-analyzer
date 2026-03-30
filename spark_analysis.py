import os
from pyspark.sql import SparkSession
from collections import Counter
import spacy
from textblob import TextBlob
import pandas as pd
import re

PYTHON_PATH = "C:\\Users\\Gokula\\OneDrive\\Desktop\\news-trend-analyzer\\sparkenv\\Scripts\\python.exe"
JAVA_HOME = "C:\\Users\\Gokula\\AppData\\Local\\Programs\\Eclipse Adoptium\\jdk-17.0.18.8-hotspot"
HADOOP_HOME = "C:\\winutils"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH
os.environ["PATH"] = f"{JAVA_HOME}\\bin;{HADOOP_HOME}\\bin;{os.environ.get('PATH', '')}"


nlp = spacy.load("en_core_web_sm")


def create_spark():
    spark = (
        SparkSession.builder.appName("NewsTrendAnalyzer")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.reuse", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_sentiment(text):
    if not text:
        return "neutral", 0.0
    score = TextBlob(text).sentiment.polarity
    if score > 0.1:
        return "positive", round(score, 2)
    elif score < -0.1:
        return "negative", round(score, 2)
    else:
        return "neutral", round(score, 2)


def extract_entities(text):
    if not text:
        return [], [], [], []
    doc = nlp(text[:1000])
    people, places, orgs, countries = [], [], [], []
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            people.append(ent.text)
        elif ent.label_ in ["GPE", "LOC"]:
            places.append(ent.text)
            countries.append(ent.text)
        elif ent.label_ == "ORG":
            orgs.append(ent.text)
    return people, places, orgs, countries


def get_keywords(text):
    if not text:
        return []
    stopwords = {
        "the",
        "a",
        "an",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "and",
        "or",
        "is",
        "was",
        "are",
        "were",
        "it",
        "its",
        "with",
        "that",
        "this",
        "by",
        "from",
        "as",
        "be",
        "has",
        "have",
        "he",
        "she",
        "they",
        "we",
        "you",
        "i",
        "but",
        "not",
        "after",
        "before",
        "about",
        "up",
        "out",
        "than",
        "more",
        "also",
        "can",
        "will",
        "just",
        "into",
        "said",
        "says",
        "say",
        "new",
        "would",
        "could",
        "their",
        "there",
        "been",
        "which",
        "when",
        "what",
        "who",
        "how",
        "his",
        "her",
        "our",
        "your",
        "over",
        "under",
        "between",
        "through",
        "during",
        "while",
        "where",
        "then",
        "than",
        "because",
        "however",
        "although",
        "though",
        "within",
        "without",
    }
    words = re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
    return [w for w in words if w not in stopwords]


def generate_summary(articles):
    if not articles:
        return "No articles found for this period."
    titles = [a.get("title", "") for a in articles if a.get("title")]
    all_text = " ".join(titles)
    keywords = get_keywords(all_text)
    top = Counter(keywords).most_common(5)
    top_words = [w for w, _ in top]
    sources = list(set([a.get("source", "") for a in articles if a.get("source")]))[:3]
    summary = (
        f"Between the selected dates, {len(articles)} articles were analyzed. "
        f"The dominant topics were {', '.join(top_words)}. "
        f"Major sources included {', '.join(sources)}. "
    )
    sentiments = [get_sentiment(a.get("title", ""))[0] for a in articles]
    neg = sentiments.count("negative")
    pos = sentiments.count("positive")
    if neg > pos:
        summary += "Overall news sentiment leaned negative during this period."
    elif pos > neg:
        summary += "Overall news sentiment leaned positive during this period."
    else:
        summary += "Overall news sentiment was mostly neutral during this period."
    return summary


def analyze(articles):
    if not articles:
        return {"error": "No articles found for this date range."}

    spark = create_spark()
    pdf = pd.DataFrame(articles)
    df = spark.createDataFrame(pdf)
    total = df.count()

    sentiments = [
        get_sentiment(
            (a.get("title", "") or "") + " " + (a.get("description", "") or "")
        )
        for a in articles
    ]
    sentiment_labels = [s[0] for s in sentiments]
    sentiment_scores = [s[1] for s in sentiments]
    sentiment_counts = Counter(sentiment_labels)
    avg_score = round(sum(sentiment_scores) / len(sentiment_scores), 2)

    all_people, all_places, all_orgs, all_countries = [], [], [], []
    for article in articles:
        text = (
            (article.get("title", "") or "")
            + " "
            + (article.get("description", "") or "")
        )
        p, pl, o, c = extract_entities(text)
        all_people.extend(p)
        all_places.extend(pl)
        all_orgs.extend(o)
        all_countries.extend(c)

    all_keywords = []
    for article in articles:
        text = (
            (article.get("title", "") or "")
            + " "
            + (article.get("description", "") or "")
        )
        all_keywords.extend(get_keywords(text))

    date_topics = {}
    for article in articles:
        date = article.get("publishedAt", "")[:10]
        text = article.get("title", "") or ""
        words = get_keywords(text)
        if date not in date_topics:
            date_topics[date] = []
        date_topics[date].extend(words)

    trending_by_date = {}
    for date, words in sorted(date_topics.items()):
        top = Counter(words).most_common(3)
        trending_by_date[date] = [w for w, _ in top]

    spark.stop()

    return {
        "total_articles": total,
        "sentiment": {
            "positive": sentiment_counts.get("positive", 0),
            "negative": sentiment_counts.get("negative", 0),
            "neutral": sentiment_counts.get("neutral", 0),
            "average_score": avg_score,
        },
        "top_keywords": Counter(all_keywords).most_common(15),
        "top_people": Counter(all_people).most_common(8),
        "top_places": Counter(all_places).most_common(8),
        "top_orgs": Counter(all_orgs).most_common(8),
        "top_countries": Counter(all_countries).most_common(8),
        "trending_by_date": trending_by_date,
        "top_sources": Counter([a.get("source", "") for a in articles]).most_common(8),
        "summary": generate_summary(articles),
        "articles_sample": articles[:5],
    }
