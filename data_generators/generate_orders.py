"""Generate fake ShopStream order events for streaming lab exercises.

Usage:
    python data_generators/generate_orders.py --events 100 --publish
    python data_generators/generate_orders.py --events 50 --output data_generators/output/

What this does:
    Generates fake order JSON events in the same format ShopStream's checkout
    service sends to Azure Event Hubs. If --publish is set, sends them to Event Hubs
    so the Structured Streaming pipeline picks them up within ~5 minutes.

    If --publish is not set, writes a JSONL file to disk for local inspection.

Format matches helix_bronze.orders.raw schema:
    order_id, customer_id, product_id, product_category, quantity, amount,
    region, status, event_timestamp
"""
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

CATEGORIES = [
    "electronics", "clothing", "home", "beauty", "sports", "books", "food",
]
REGIONS = [
    "nl-north", "nl-south", "nl-east", "nl-west", "nl-central",
    "be-flanders", "be-wallonia", "de-north", "de-south",
]
STATUSES = ["placed", "confirmed"]


def _make_order() -> dict:
    return {
        "order_id": f"ORD{uuid.uuid4().hex[:7].upper()}",
        "customer_id": f"C{random.randint(1, 50000):06d}",
        "product_id": f"P{random.randint(1, 5000):05d}",
        "product_category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "region": random.choice(REGIONS),
        "status": random.choice(STATUSES),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def generate_orders(n: int, output_dir: Path) -> Path:
    """Write n order events to a JSONL file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"orders_{today}.jsonl"

    with open(output_path, "w", encoding="utf-8") as f:
        for _ in range(n):
            f.write(json.dumps(_make_order()) + "\n")

    print(f"Generated {n} orders → {output_path}")
    return output_path


def publish_to_event_hubs(n: int, connection_string: str, hub_name: str) -> None:
    """Publish n order events to Azure Event Hubs.

    Requires: pip install azure-eventhub
    """
    try:
        from azure.eventhub import EventData, EventHubProducerClient  # type: ignore[import]
    except ImportError:
        raise SystemExit("Install azure-eventhub first: pip install azure-eventhub")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string, eventhub_name=hub_name
    )
    with producer:
        batch = producer.create_batch()
        for _ in range(n):
            event_data = EventData(json.dumps(_make_order()))
            batch.add(event_data)
        producer.send_batch(batch)

    print(f"Published {n} order events to Event Hubs hub={hub_name}")


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Generate ShopStream order events")
    parser.add_argument("--events", type=int, default=100, help="Number of events")
    parser.add_argument("--output", type=str, default="data_generators/output")
    parser.add_argument("--publish", action="store_true", help="Publish to Event Hubs")
    args = parser.parse_args()

    if args.publish:
        conn_str = os.environ["EVENT_HUBS_CONNECTION_STRING"]
        hub_name = os.environ.get("EVENT_HUBS_NAME", "orders-stream")
        publish_to_event_hubs(args.events, conn_str, hub_name)
    else:
        generate_orders(args.events, output_dir=Path(args.output))
