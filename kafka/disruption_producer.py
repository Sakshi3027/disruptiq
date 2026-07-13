"""
DisruptIQ — Kafka Disruption Event Producer
Fetches real supply chain news from NewsAPI
Classifies disruption type and severity
Streams to Kafka topic: disruptiq_events
"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv
import re

load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "disruptiq_events"

# Supply chain disruption keywords
DISRUPTION_KEYWORDS = [
    "supply chain disruption", "port delay", "shipping delay",
    "factory shutdown", "semiconductor shortage", "freight",
    "cargo", "logistics disruption", "trade route", "tariff",
    "manufacturing halt", "chip shortage", "container shortage",
    "port congestion", "supply shortage", "export ban"
]

# Disruption type classification
DISRUPTION_TYPES = {
    "port": ["port", "harbor", "shipping", "cargo", "container", "freight", "vessel"],
    "weather": ["hurricane", "typhoon", "flood", "earthquake", "storm", "cyclone"],
    "geopolitical": ["tariff", "sanction", "trade war", "ban", "embargo", "conflict"],
    "pandemic": ["covid", "lockdown", "quarantine", "outbreak", "pandemic"],
    "factory": ["factory", "plant", "manufacturing", "production halt", "shutdown"],
    "semiconductor": ["chip", "semiconductor", "wafer", "foundry", "tsmc", "nvidia"],
}

# Port mapping for affected regions
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

def classify_disruption(title: str, description: str) -> dict:
    """Classify disruption type and severity."""
    text = (title + " " + (description or "")).lower()

    # Determine disruption type
    disruption_type = "general"
    for dtype, keywords in DISRUPTION_TYPES.items():
        if any(kw in text for kw in keywords):
            disruption_type = dtype
            break

    # Determine affected ports
    affected_ports = []
    for port_code, keywords in PORT_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            affected_ports.append(port_code)

    # Calculate severity score (0-1)
    severity_keywords = {
        "critical": ["shutdown", "halt", "blocked", "closed", "crisis", "emergency"],
        "high": ["disruption", "shortage", "delay", "ban", "sanction"],
        "medium": ["concern", "risk", "warning", "slowdown"],
        "low": ["monitor", "watch", "potential"]
    }

    severity = 0.3  # default
    for level, keywords in severity_keywords.items():
        if any(kw in text for kw in keywords):
            severity = {"critical": 0.9, "high": 0.7, "medium": 0.5, "low": 0.3}[level]
            break

    return {
        "disruption_type": disruption_type,
        "affected_ports": affected_ports,
        "severity": severity
    }

def fetch_supply_chain_news() -> list:
    """Fetch real supply chain news from NewsAPI."""
    events = []

    for keyword in DISRUPTION_KEYWORDS[:5]:  # Limit API calls
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
                        "affected_ports": classification["affected_ports"],
                        "severity": classification["severity"],
                        "keyword_trigger": keyword
                    }
                    events.append(event)
            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            print(f"  ⚠️ Error fetching '{keyword}': {e}")
            continue

    return events

def run_producer():
    print("🚨 DisruptIQ — Disruption Event Producer")
    print(f"   Topic: {KAFKA_TOPIC}")
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
        print(f"  ✅ Sent {sent} disruption events to Kafka")
        print(f"  🚨 High severity: {high_severity}")
        if events:
            print(f"  📰 Sample: {events[0]['title'][:70]}...")
            print(f"     Type: {events[0]['disruption_type']} | Severity: {events[0]['severity']}")
            print(f"     Affected ports: {events[0]['affected_ports']}")
        print(f"  ⏳ Next batch in 300 seconds (5 min)...")
        time.sleep(300)

if __name__ == "__main__":
    run_producer()
