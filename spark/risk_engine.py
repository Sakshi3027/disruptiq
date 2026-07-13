"""
DisruptIQ — Risk Propagation Engine
Reads disruption events from Kafka
Queries Neo4j knowledge graph to find affected companies
Calculates and propagates risk scores
"""

import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from dotenv import load_dotenv
import threading

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "disruptiq_events"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "disruptiq123")

# In-memory risk store (replace with Cassandra in production)
risk_store = {}
alert_log = []

class RiskPropagationEngine:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        print("✅ Connected to Neo4j Knowledge Graph")

    def get_affected_companies(self, port_codes: list) -> list:
        """Query Neo4j for companies affected by disrupted ports."""
        if not port_codes:
            return []
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Company)-[:SHIPS_THROUGH]->(p:Port)
                WHERE p.code IN $port_codes
                RETURN DISTINCT
                    c.name as company,
                    c.sector as sector,
                    c.country as country,
                    c.risk_score as base_risk,
                    collect(p.name) as affected_ports
                ORDER BY c.risk_score DESC
            """, port_codes=port_codes)
            return [dict(r) for r in result]

    def get_downstream_impact(self, company: str) -> list:
        """Find products affected when a company is disrupted."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Company {name: $company})-[r]->(p:Product)
                RETURN p.name as product,
                       p.criticality as criticality,
                       type(r) as relationship
            """, company=company)
            return [dict(r) for r in result]

    def calculate_risk_score(self, base_risk: float,
                              severity: float,
                              disruption_type: str) -> float:
        """Calculate composite risk score."""
        type_multipliers = {
            "semiconductor": 1.5,
            "port": 1.3,
            "geopolitical": 1.4,
            "weather": 1.2,
            "factory": 1.3,
            "pandemic": 1.6,
            "general": 1.0
        }
        multiplier = type_multipliers.get(disruption_type, 1.0)
        risk_score = min(1.0, base_risk * severity * multiplier * 2)
        return round(risk_score, 3)

    def propagate_risk(self, event: dict) -> dict:
        """Main risk propagation logic."""
        port_codes = event.get("affected_ports", [])
        severity = event.get("severity", 0.3)
        disruption_type = event.get("disruption_type", "general")

        # Find affected companies via Neo4j
        affected_companies = self.get_affected_companies(port_codes)

        impacts = []
        for company in affected_companies:
            risk_score = self.calculate_risk_score(
                company["base_risk"],
                severity,
                disruption_type
            )

            # Get downstream product impact
            products = self.get_downstream_impact(company["company"])

            impact = {
                "company": company["company"],
                "sector": company["sector"],
                "country": company["country"],
                "risk_score": risk_score,
                "affected_ports": company["affected_ports"],
                "impacted_products": [p["product"] for p in products],
                "alert_level": "🔴 CRITICAL" if risk_score >= 0.7
                               else "🟡 HIGH" if risk_score >= 0.5
                               else "🟢 MEDIUM"
            }
            impacts.append(impact)

            # Update risk store
            if company["company"] not in risk_store or \
               risk_store[company["company"]]["risk_score"] < risk_score:
                risk_store[company["company"]] = impact

        result = {
            "event_id": event.get("event_id"),
            "title": event.get("title", "")[:80],
            "disruption_type": disruption_type,
            "severity": severity,
            "affected_ports": port_codes,
            "company_impacts": impacts,
            "total_companies_affected": len(impacts),
            "processed_at": datetime.now().isoformat()
        }

        if impacts:
            alert_log.append(result)

        return result

    def close(self):
        self.driver.close()

def print_risk_report():
    """Print current risk state."""
    print("\n" + "="*60)
    print("🚨 DisruptIQ — LIVE RISK DASHBOARD")
    print("="*60)
    if not risk_store:
        print("  No risks detected yet...")
        return

    sorted_risks = sorted(
        risk_store.values(),
        key=lambda x: x["risk_score"],
        reverse=True
    )

    print(f"\n{'Company':<15} {'Sector':<18} {'Risk':>6} {'Alert':<15}")
    print("-"*60)
    for r in sorted_risks:
        print(f"{r['company']:<15} {r['sector']:<18} "
              f"{r['risk_score']:>5.1%} {r['alert_level']}")
        if r["impacted_products"]:
            print(f"  └─ Products: {', '.join(r['impacted_products'][:3])}")
    print("="*60)

def run_risk_engine():
    engine = RiskPropagationEngine()

    print("\n⚡ DisruptIQ Risk Propagation Engine Started")
    print(f"   Listening to Kafka topic: {KAFKA_TOPIC}")
    print(f"   Knowledge graph: Neo4j")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="disruptiq_risk_engine",
        consumer_timeout_ms=5000
    )

    processed = 0
    print("\n📨 Processing disruption events...\n")

    try:
        for message in consumer:
            event = message.value
            result = engine.propagate_risk(event)
            processed += 1

            if result["total_companies_affected"] > 0:
                print(f"🚨 [{processed}] {result['title'][:60]}...")
                print(f"   Type: {result['disruption_type']} | "
                      f"Severity: {result['severity']:.0%} | "
                      f"Companies affected: {result['total_companies_affected']}")
                for impact in result["company_impacts"][:3]:
                    print(f"   {impact['alert_level']} {impact['company']} "
                          f"— Risk: {impact['risk_score']:.0%}")
                print()
            else:
                print(f"ℹ️  [{processed}] {result['title'][:60]}... "
                      f"(no port impact)")

    except Exception as e:
        print(f"\nConsumer timeout or error: {e}")

    print_risk_report()

    # Save results
    os.makedirs("data", exist_ok=True)
    with open("data/risk_report.json", "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "total_events_processed": processed,
            "company_risks": list(risk_store.values()),
            "alerts": alert_log[:10]
        }, f, indent=2)

    print(f"\n✅ Risk report saved to data/risk_report.json")
    engine.close()

if __name__ == "__main__":
    run_risk_engine()
