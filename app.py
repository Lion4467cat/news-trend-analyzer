from flask import Flask, render_template, request, jsonify
from kafka_producer import fetch_and_send
from kafka_consumer import consume_articles
from dask_analysis import analyze

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze_news():
    from_date = request.form.get("from_date")
    to_date = request.form.get("to_date")
    query = request.form.get("query") or "world news"

    if not from_date or not to_date:
        return jsonify({"error": "Please select both dates!"}), 400

    # Step 1: Fetch news and send to Kafka
    count = fetch_and_send(from_date, to_date, query)
    if count == 0:
        return render_template(
            "result.html",
            error="No articles found for this date range. Try different dates or keywords.",
        )

    # Step 2: Consume from Kafka
    articles = consume_articles()
    if not articles:
        return render_template(
            "result.html", error="Could not consume articles from Kafka."
        )

    # Step 3: Analyze with Spark
    results = analyze(articles)

    return render_template(
        "result.html",
        results=results,
        from_date=from_date,
        to_date=to_date,
        query=query,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
