"""
DisruptIQ — FastAPI Backend
Serves supply chain risk intelligence data
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from dotenv import load_dotenv
import json
import os
from datetime import datetime
import pytz

load_dotenv()

app = FastAPI(title="DisruptIQ API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "disruptiq123")

def get_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def load_risk_report():
    if os.path.exists("data/risk_report.json"):
        with open("data/risk_report.json") as f:
            return json.load(f)
    return None

@app.get("/")
def root():
    return {"name": "DisruptIQ", "tagline": "Real-time supply chain risk intelligence"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/risks")
def risks():
    report = load_risk_report()
    if not report:
        return {"company_risks": [], "total_events": 0}
    return {
        "generated_at": report.get("generated_at"),
        "total_events_processed": report.get("total_events_processed", 0),
        "company_risks": sorted(
            report.get("company_risks", []),
            key=lambda x: x["risk_score"],
            reverse=True
        )
    }

@app.get("/alerts")
def alerts():
    report = load_risk_report()
    if not report:
        return {"alerts": []}
    return {"alerts": report.get("alerts", [])[:10]}

@app.get("/graph/affected/{port_code}")
def affected_by_port(port_code: str):
    """Get companies affected by a disrupted port."""
    driver = get_neo4j()
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Company)-[:SHIPS_THROUGH]->(p:Port {code: $port_code})
            RETURN c.name as company, c.sector as sector,
                   c.risk_score as risk_score, p.name as port
            ORDER BY c.risk_score DESC
        """, port_code=port_code)
        companies = [dict(r) for r in result]
    driver.close()
    return {"port": port_code, "affected_companies": companies}

@app.get("/graph/stats")
def graph_stats():
    """Get knowledge graph statistics."""
    driver = get_neo4j()
    with driver.session() as session:
        companies = session.run("MATCH (c:Company) RETURN count(c) as count").single()["count"]
        ports = session.run("MATCH (p:Port) RETURN count(p) as count").single()["count"]
        products = session.run("MATCH (pr:Product) RETURN count(pr) as count").single()["count"]
        relationships = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
    driver.close()
    return {
        "companies": companies,
        "ports": ports,
        "products": products,
        "relationships": relationships
    }

@app.get("/graph/ports")
def get_ports():
    """Get all ports with risk scores."""
    driver = get_neo4j()
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Port)
            RETURN p.code as code, p.name as name,
                   p.country as country, p.risk_score as risk_score,
                   p.volume_teu as volume_teu
            ORDER BY p.risk_score DESC
        """)
        ports = [dict(r) for r in result]
    driver.close()
    return {"ports": ports}
