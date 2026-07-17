"""
DisruptIQ — Kafka Disruption Event Producer
Fetches real supply chain news from NewsAPI
Uses fine-tuned DistilBERT model for disruption classification
Streams to Kafka topic: disruptiq_events
"""

import requests
import json
import time
import os
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv
import re

sys.path.insert(0, ".")
from llm.classifier import DisruptionClassifier

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "disruptiq_events"

DISRUPTION_KEYWORDS = [
    "supply chain disruption", "port delay", "shipping delay",
    "factory shutdown", "semiconductor shortage", "freight",
    "cargo", "logistics disruption", "trade route", "tariff",
    "manufacturing halt", "chip shortage", "container shortage",
    "port congestion", "supply shortage", "export ban"
]

PORT_KEYWORDS = {
    "CNSHA": ["shanghai", "china", "chinese"],
    "SGSIN": ["singapore"],
    "CNSZX": ["shenzhen"],
    "NLRTM": ["rotterdam", "netherlands", "europe"],
    "USLA": ["los angeles", "long beach", "california", "west coast"],
    "JPYOK": ["japan", "tokyo", "yokohama"],
    "KRPUS": ["busan", "korea", "korean"],
    "TWKHH": ["taiwan", "kaohsiung", "taipei"],
    "EGPSE": ["suez", "egypt", "red sea"],
    "USNYC": ["new york", "east coast"],
}

SEVERITY_KEYWORDS = {
    "critical": ["shutdown", "halt", "blocked", "closed", "crisis", "emergency"],
    "high": ["disruption", "shortage", "delay", "ban", "sanction"],
    "medium": ["concern", "risk", "warning", "slowdown"],
    "low": ["monitor", "watch", "potential"]
}

# Load fine-tuned model once at startup
print("🤖 Loading fine-tuned DisruptIQ classifier...")
_ml_classifier = DisruptionClassifier()

def get_affected_ports(text: str) -> list:
    """Extract affected ports from text using keyword matching."""
    text_lower = text.lower()
    affected_ports = []
    for port_code, keywords in PORT_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            affected_ports.append(port_code)
    return affected_ports

def get_severity(text: str) -> float:
    """Calculate severity score from text."""
    text_lower = text.lower()
    for level, keywords in SEVERITY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return {"critical": 0.9, "high": 0.7, "medium": 0.5, "low": 0.3}[level]
    return 0.3

def classify_disruption(title: str, description: str) -> dict:
    """
    Classify disruption using fine-tuned DistilBERT model.
    Port detection and severity still use keyword matching.
    """
    full_text = title + " " + (description or "")

    # ML classification for disruption type
    ml_result = _ml_classifier.classify(full_text)

    # Keyword matching for ports and severity
    affected_ports = get_affected_ports(full_text)
    severity = get_severity(full_text)

    return {
        "disruption_type": ml_result["disruption_type"],
        "ml_confidence": ml_result["confidence"],
        "affected_ports": affected_ports,
        "severity": severity
    }

def fetch_supply_chain_news() -> list:
    """Fetch real supply chain news from NewsAPI."""
    events = []

    for keyword in DISRUPTION_KEYWORDS[:5]:
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": keyword,
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 5,
                "from": (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d"),
                "apiKey": NEWS_API_KEY
            }
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if data.get("status") == "ok":
                for article in data.get("articles", []):
                    if not article.get("title"):
                        continue

                    classification = classify_disruption(
                        article["title"],
                        article.get("description", "")
                    )

                    event = {
                        "event_id": f"EVT_{int(time.time())}_{len(events)}",
                        "title": article["title"],
                        "description": article.get("description", "")[:200],
                        "source": article.get("source", {}).get("name", "Unknown"),
                        "url": article.get("url", ""),
                        "published_at": article.get("publishedAt", ""),
                        "ingested_at": datetime.now().isoformat(),
                        "disruption_type": classification["disruption_type"],
                        "ml_confidence": classification["ml_confidence"],
                        "affected_ports": classification["affected_ports"],
                        "severity": classification["severity"],
                        "keyword_trigger": keyword
                    }
                    events.append(event)
            time.sleep(0.5)

        except Exception as e:
            print(f"  ⚠️ Error fetching '{keyword}': {e}")
            continue

    return events

def run_producer():
    print("\n🚨 DisruptIQ — Disruption Event Producer")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Classifier: Fine-tuned DistilBERT")
    print(f"   NewsAPI: {'✅ configured' if NEWS_API_KEY else '❌ missing'}")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )

    batch = 0
    while True:
        batch += 1
        print(f"\n📡 Batch {batch} — Fetching supply chain news...")
        events = fetch_supply_chain_news()

        if not events:
            print("  No events fetched")
            time.sleep(60)
            continue

        sent = 0
        high_severity = 0
        for event in events:
            producer.send(
                KAFKA_TOPIC,
                key=event["event_id"],
                value=event
            )
            sent += 1
            if event["severity"] >= 0.7:
                high_severity += 1

        producer.flush()
        print(f"  ✅ Sent {sent} events to Kafka")
        print(f"  🚨 High severity: {high_severity}")
        if events:
            print(f"  📰 Sample: {events[0]['title'][:60]}...")
            print(f"     ML Type: {events[0]['disruption_type']} ({events[0]['ml_confidence']:.0%} confidence)")
            print(f"     Severity: {events[0]['severity']}")
            print(f"     Ports: {events[0]['affected_ports']}")
        print(f"  ⏳ Next batch in 300 seconds...")
        time.sleep(300)

if __name__ == "__main__":
    run_producer()
