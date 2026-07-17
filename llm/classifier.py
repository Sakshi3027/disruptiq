"""
DisruptIQ — Fine-tuned Disruption Classifier
Uses Sakshi3027/disruptiq-supply-chain-classifier
Fine-tuned DistilBERT on supply chain disruption data
Replaces keyword matching with ML classification
"""

from transformers import pipeline

MODEL_NAME = "Sakshi3027/disruptiq-supply-chain-classifier"

class DisruptionClassifier:
    def __init__(self):
        print(f"🤖 Loading fine-tuned model: {MODEL_NAME}")
        self.classifier = pipeline(
            "text-classification",
            model=MODEL_NAME,
            device=-1
        )
        print("✅ Fine-tuned classifier loaded!")

    def classify(self, text: str) -> dict:
        """Classify a news headline into disruption type."""
        result = self.classifier(text[:512], truncation=True)[0]
        return {
            "disruption_type": result["label"],
            "confidence": round(result["score"], 3)
        }

    def classify_batch(self, texts: list) -> list:
        results = self.classifier(
            [t[:512] for t in texts],
            truncation=True,
            batch_size=8
        )
        return [
            {
                "disruption_type": r["label"],
                "confidence": round(r["score"], 3)
            }
            for r in results
        ]

if __name__ == "__main__":
    clf = DisruptionClassifier()

    test_headlines = [
        "Shanghai port congestion causes major shipping delays",
        "TSMC chip shortage will continue into next quarter",
        "US imposes new tariffs on Chinese semiconductor imports",
        "Typhoon forces closure of Taiwan manufacturing plants",
        "Apple supplier shuts factory amid worker protests",
        "Red Sea attacks force ships to reroute around Africa",
        "Markets react to Fed interest rate decision",
    ]

    print("\n🔍 DisruptIQ Fine-tuned Classifier Results:")
    print("="*60)
    for headline in test_headlines:
        result = clf.classify(headline)
        print(f"📰 {headline[:55]}...")
        print(f"   → {result['disruption_type'].upper()} ({result['confidence']:.0%} confidence)")
        print()
