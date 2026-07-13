"""
DisruptIQ — Neo4j Knowledge Graph
Models supply chain relationships:
Company → supplies → Product
Company → ships_through → Port
Port → connects → Port (trade routes)
Event → disrupts → Port/Region
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import json

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "disruptiq123")

class SupplyChainGraph:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
        print("✅ Connected to Neo4j")

    def close(self):
        self.driver.close()

    def clear_and_setup(self):
        """Create constraints and indexes."""
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT company_name IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE")
            session.run("CREATE CONSTRAINT port_code IF NOT EXISTS FOR (p:Port) REQUIRE p.code IS UNIQUE")
            session.run("CREATE CONSTRAINT product_id IF NOT EXISTS FOR (pr:Product) REQUIRE pr.id IS UNIQUE")
            session.run("CREATE CONSTRAINT region_name IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE")
            print("✅ Constraints created")

    def load_supply_chain_data(self):
        """Load real supply chain entities into Neo4j."""
        
        # Major global companies
        companies = [
            {"name": "Apple", "sector": "Technology", "country": "USA", "risk_score": 0.3},
            {"name": "Samsung", "sector": "Technology", "country": "South Korea", "risk_score": 0.25},
            {"name": "Toyota", "sector": "Automotive", "country": "Japan", "risk_score": 0.2},
            {"name": "Nike", "sector": "Apparel", "country": "USA", "risk_score": 0.4},
            {"name": "TSMC", "sector": "Semiconductors", "country": "Taiwan", "risk_score": 0.6},
            {"name": "Foxconn", "sector": "Manufacturing", "country": "Taiwan", "risk_score": 0.55},
            {"name": "Amazon", "sector": "E-commerce", "country": "USA", "risk_score": 0.2},
            {"name": "Tesla", "sector": "Automotive", "country": "USA", "risk_score": 0.35},
            {"name": "NVIDIA", "sector": "Semiconductors", "country": "USA", "risk_score": 0.45},
            {"name": "Walmart", "sector": "Retail", "country": "USA", "risk_score": 0.3},
        ]

        # Major global ports
        ports = [
            {"code": "CNSHA", "name": "Shanghai", "country": "China", "volume_teu": 47300000, "risk_score": 0.3},
            {"code": "SGSIN", "name": "Singapore", "country": "Singapore", "volume_teu": 37500000, "risk_score": 0.15},
            {"code": "CNSZX", "name": "Shenzhen", "country": "China", "volume_teu": 30000000, "risk_score": 0.3},
            {"code": "NLRTM", "name": "Rotterdam", "country": "Netherlands", "volume_teu": 15300000, "risk_score": 0.1},
            {"code": "USLA", "name": "Los Angeles", "country": "USA", "volume_teu": 10700000, "risk_score": 0.2},
            {"code": "JPYOK", "name": "Yokohama", "country": "Japan", "volume_teu": 2900000, "risk_score": 0.15},
            {"code": "KRPUS", "name": "Busan", "country": "South Korea", "volume_teu": 22000000, "risk_score": 0.15},
            {"code": "TWKHH", "name": "Kaohsiung", "country": "Taiwan", "volume_teu": 10400000, "risk_score": 0.4},
            {"code": "EGPSE", "name": "Port Said", "country": "Egypt", "volume_teu": 4000000, "risk_score": 0.5},
            {"code": "USNYC", "name": "New York", "country": "USA", "volume_teu": 9500000, "risk_score": 0.15},
        ]

        # Products
        products = [
            {"id": "semiconductors", "name": "Semiconductors", "criticality": "HIGH"},
            {"id": "smartphones", "name": "Smartphones", "criticality": "HIGH"},
            {"id": "automotive_parts", "name": "Automotive Parts", "criticality": "HIGH"},
            {"id": "apparel", "name": "Apparel & Footwear", "criticality": "MEDIUM"},
            {"id": "electronics", "name": "Consumer Electronics", "criticality": "HIGH"},
            {"id": "ev_batteries", "name": "EV Batteries", "criticality": "HIGH"},
            {"id": "retail_goods", "name": "Retail Goods", "criticality": "MEDIUM"},
        ]

        # Regions
        regions = [
            {"name": "Asia Pacific", "risk_level": "HIGH"},
            {"name": "Europe", "risk_level": "MEDIUM"},
            {"name": "North America", "risk_level": "LOW"},
            {"name": "Middle East", "risk_level": "HIGH"},
            {"name": "Southeast Asia", "risk_level": "MEDIUM"},
        ]

        with self.driver.session() as session:
            # Create companies
            for company in companies:
                session.run("""
                    MERGE (c:Company {name: $name})
                    SET c.sector = $sector,
                        c.country = $country,
                        c.risk_score = $risk_score
                """, **company)

            # Create ports
            for port in ports:
                session.run("""
                    MERGE (p:Port {code: $code})
                    SET p.name = $name,
                        p.country = $country,
                        p.volume_teu = $volume_teu,
                        p.risk_score = $risk_score
                """, **port)

            # Create products
            for product in products:
                session.run("""
                    MERGE (pr:Product {id: $id})
                    SET pr.name = $name,
                        pr.criticality = $criticality
                """, **product)

            # Create regions
            for region in regions:
                session.run("""
                    MERGE (r:Region {name: $name})
                    SET r.risk_level = $risk_level
                """, **region)

            print("✅ Nodes created")

            # Create supply relationships
            relationships = [
                # Company SUPPLIES Product
                ("Apple", "smartphones", "SUPPLIES", {"volume": "HIGH", "dependency": 0.9}),
                ("Apple", "semiconductors", "DEPENDS_ON", {"volume": "HIGH", "dependency": 0.95}),
                ("TSMC", "semiconductors", "SUPPLIES", {"volume": "HIGH", "dependency": 1.0}),
                ("Foxconn", "smartphones", "MANUFACTURES", {"volume": "HIGH", "dependency": 0.8}),
                ("Samsung", "semiconductors", "SUPPLIES", {"volume": "HIGH", "dependency": 0.7}),
                ("Samsung", "smartphones", "SUPPLIES", {"volume": "HIGH", "dependency": 0.8}),
                ("Toyota", "automotive_parts", "MANUFACTURES", {"volume": "HIGH", "dependency": 0.7}),
                ("Tesla", "ev_batteries", "DEPENDS_ON", {"volume": "HIGH", "dependency": 0.9}),
                ("Nike", "apparel", "SUPPLIES", {"volume": "HIGH", "dependency": 0.6}),
                ("NVIDIA", "semiconductors", "DEPENDS_ON", {"volume": "HIGH", "dependency": 0.95}),
                ("Amazon", "retail_goods", "DISTRIBUTES", {"volume": "HIGH", "dependency": 0.5}),
                ("Walmart", "retail_goods", "DISTRIBUTES", {"volume": "HIGH", "dependency": 0.6}),
            ]

            for company, product, rel_type, props in relationships:
                session.run(f"""
                    MATCH (c:Company {{name: $company}})
                    MATCH (p:Product {{id: $product}})
                    MERGE (c)-[r:{rel_type}]->(p)
                    SET r += $props
                """, company=company, product=product, props=props)

            # Company SHIPS_THROUGH Port
            shipping_routes = [
                ("Apple", "CNSHA"), ("Apple", "TWKHH"),
                ("TSMC", "TWKHH"), ("TSMC", "CNSHA"),
                ("Foxconn", "CNSHA"), ("Foxconn", "TWKHH"),
                ("Samsung", "KRPUS"), ("Samsung", "SGSIN"),
                ("Toyota", "JPYOK"), ("Toyota", "USLA"),
                ("Tesla", "USLA"), ("Tesla", "CNSHA"),
                ("Nike", "CNSHA"), ("Nike", "SGSIN"),
                ("NVIDIA", "TWKHH"), ("NVIDIA", "SGSIN"),
                ("Amazon", "USLA"), ("Amazon", "USNYC"),
                ("Walmart", "USLA"), ("Walmart", "NLRTM"),
            ]

            for company, port_code in shipping_routes:
                session.run("""
                    MATCH (c:Company {name: $company})
                    MATCH (p:Port {code: $port_code})
                    MERGE (c)-[:SHIPS_THROUGH]->(p)
                """, company=company, port_code=port_code)

            # Port CONNECTS Port (trade routes)
            trade_routes = [
                ("CNSHA", "USLA"), ("CNSHA", "NLRTM"),
                ("SGSIN", "NLRTM"), ("SGSIN", "USLA"),
                ("TWKHH", "USLA"), ("TWKHH", "JPYOK"),
                ("KRPUS", "USLA"), ("KRPUS", "NLRTM"),
                ("EGPSE", "NLRTM"), ("EGPSE", "SGSIN"),
                ("JPYOK", "USLA"), ("JPYOK", "NLRTM"),
            ]

            for port1, port2 in trade_routes:
                session.run("""
                    MATCH (p1:Port {code: $port1})
                    MATCH (p2:Port {code: $port2})
                    MERGE (p1)-[:TRADE_ROUTE]->(p2)
                    MERGE (p2)-[:TRADE_ROUTE]->(p1)
                """, port1=port1, port2=port2)

            print("✅ Relationships created")

    def get_affected_companies(self, port_code: str) -> list:
        """Given a disrupted port, find all affected companies."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Company)-[:SHIPS_THROUGH]->(p:Port {code: $port_code})
                RETURN c.name as company, c.sector as sector, c.risk_score as risk_score
                ORDER BY c.risk_score DESC
            """, port_code=port_code)
            return [dict(r) for r in result]

    def get_supply_chain_path(self, company: str, product: str) -> list:
        """Find supply chain path from company to product."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH path = (c:Company {name: $company})-[*1..3]->(pr:Product {id: $product})
                RETURN path
                LIMIT 5
            """, company=company, product=product)
            return [dict(r) for r in result]

    def get_graph_stats(self) -> dict:
        """Get knowledge graph statistics."""
        with self.driver.session() as session:
            companies = session.run("MATCH (c:Company) RETURN count(c) as count").single()["count"]
            ports = session.run("MATCH (p:Port) RETURN count(p) as count").single()["count"]
            products = session.run("MATCH (pr:Product) RETURN count(pr) as count").single()["count"]
            relationships = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]

            return {
                "companies": companies,
                "ports": ports,
                "products": products,
                "relationships": relationships
            }

def build_knowledge_graph():
    graph = SupplyChainGraph()
    graph.clear_and_setup()
    graph.load_supply_chain_data()

    stats = graph.get_graph_stats()
    print(f"\n📊 Knowledge Graph Statistics:")
    print(f"   Companies:     {stats['companies']}")
    print(f"   Ports:         {stats['ports']}")
    print(f"   Products:      {stats['products']}")
    print(f"   Relationships: {stats['relationships']}")

    # Test query
    print(f"\n🔍 Companies affected if Shanghai port is disrupted:")
    affected = graph.get_affected_companies("CNSHA")
    for company in affected:
        print(f"   - {company['company']} ({company['sector']}) risk: {company['risk_score']}")

    graph.close()
    print("\n✅ Knowledge graph built successfully!")

if __name__ == "__main__":
    build_knowledge_graph()
