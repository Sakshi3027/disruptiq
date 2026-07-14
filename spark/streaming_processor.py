"""
DisruptIQ — Apache Spark Structured Streaming
Reads disruption events from Kafka
Processes in real-time using Spark micro-batches
Writes risk scores to output
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import json
import builtins as _builtins

load_dotenv()

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "disruptiq_events"
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("source", StringType()),
    StructField("published_at", StringType()),
    StructField("ingested_at", StringType()),
    StructField("disruption_type", StringType()),
    StructField("affected_ports", ArrayType(StringType())),
    StructField("severity", DoubleType()),
    StructField("keyword_trigger", StringType())
])

def get_affected_companies(port_codes: list) -> list:
    if not port_codes:
        return []
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Company)-[:SHIPS_THROUGH]->(p:Port)
            WHERE p.code IN $port_codes
            RETURN DISTINCT c.name as company,
                   c.sector as sector,
                   c.risk_score as base_risk,
                   collect(p.name) as ports
            ORDER BY c.risk_score DESC
        """, port_codes=port_codes)
        companies = [dict(r) for r in result]
    driver.close()
    return companies

def process_batch(df, epoch_id):
    """Process each Spark micro-batch."""
    if df.rdd.isEmpty():
        return

    rows = df.collect()
    print(f"\n⚡ [Epoch {epoch_id}] Processing {len(rows)} disruption events...")

    for row in rows:
        port_codes = list(row["affected_ports"]) if row["affected_ports"] else []
        severity = float(row["severity"])
        disruption_type = row["disruption_type"]

        companies = get_affected_companies(port_codes)

        if companies:
            print(f"\n🚨 DISRUPTION DETECTED:")
            print(f"   Title: {row['title'][:70]}...")
            print(f"   Type: {disruption_type} | Severity: {severity:.0%}")
            print(f"   Affected ports: {port_codes}")
            print(f"   Companies at risk:")

            type_multipliers = {
                "semiconductor": 1.5, "port": 1.3,
                "geopolitical": 1.4, "weather": 1.2,
                "factory": 1.3, "pandemic": 1.6, "general": 1.0
            }
            multiplier = type_multipliers.get(disruption_type, 1.0)

            for company in companies:
                risk_score = _builtins.min(1.0, company["base_risk"] * severity * multiplier * 2)
                alert = "🔴 CRITICAL" if risk_score >= 0.7 else "🟡 HIGH" if risk_score >= 0.5 else "🟢 MEDIUM"
                print(f"   {alert} {company['company']} ({company['sector']}) — Risk: {risk_score:.0%}")
        else:
            print(f"ℹ️  [{epoch_id}] {row['title'][:60]}... (no port impact)")

def run_spark_streaming():
    print("⚡ DisruptIQ — Spark Structured Streaming")
    print("=" * 55)

    spark = SparkSession.builder \
        .appName("DisruptIQ-Streaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.streaming.checkpointLocation",
                "/tmp/disruptiq_checkpoint") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"  Spark version: {spark.version}")
    print(f"  Kafka topic: {KAFKA_TOPIC}")
    print(f"  Neo4j: {NEO4J_URI}")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed = raw_stream.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    # Write stream via foreachBatch
    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .start()

    print("\n✅ Spark Streaming started — processing every 15 seconds")
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_streaming()
