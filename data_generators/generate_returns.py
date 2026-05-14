"""Generate fake ShopStream order returns CSV for Auto Loader lab exercises.

Usage:
    python data_generators/generate_returns.py --rows 200 --output data_generators/output/

What this does:
    Writes a CSV file of ShopStream order returns.
    This simulates the nightly export from the Refund Management System (RMS).
    The Auto Loader pipeline (ingest_returns_autoloader.py) reads these files.

    New file each run = new file in ADLS Gen2 = Auto Loader picks it up.
    Checkpoint prevents re-reading the old file.

Why returns come as files (not Lakeflow Connect):
    Returns are handled by a separate system (RMS) that is NOT a live database
    Databricks can connect to via JDBC. It exports a CSV every night to ADLS Gen2.
    This is the textbook Auto Loader scenario.
"""
import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

REASONS = [
    "damaged", "wrong_size", "wrong_item", "changed_mind",
    "not_as_described", "arrived_late",
]
REASON_WEIGHTS = [0.2, 0.25, 0.1, 0.25, 0.15, 0.05]

STATUSES = ["pending", "approved", "rejected"]
STATUS_WEIGHTS = [0.2, 0.7, 0.1]


def _random_date(start: date, end: date) -> date:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_returns(rows: int, output_dir: Path) -> Path:
    """Generate a CSV file of fake returns."""
    output_dir.mkdir(parents=True, exist_ok=True)
    today = date.today()
    output_path = output_dir / f"returns_{today.strftime('%Y%m%d')}.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "return_id", "order_id", "customer_id", "product_id",
                "return_date", "reason", "refund_amount", "status",
            ],
        )
        writer.writeheader()

        for i in range(1, rows + 1):
            return_date = _random_date(date(2026, 1, 1), today)
            writer.writerow({
                "return_id": f"R{uuid.uuid4().hex[:8].upper()}",
                "order_id": f"ORD{random.randint(1, 100000):07d}",
                "customer_id": f"C{random.randint(1, 50000):06d}",
                "product_id": f"P{random.randint(1, 5000):05d}",
                "return_date": str(return_date),
                "reason": random.choices(REASONS, weights=REASON_WEIGHTS, k=1)[0],
                "refund_amount": round(random.uniform(5.0, 500.0), 2),
                "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
            })

    print(f"Generated {rows} returns → {output_path}")
    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate ShopStream returns CSV")
    parser.add_argument("--rows", type=int, default=200, help="Number of rows to generate")
    parser.add_argument("--output", type=str, default="data_generators/output", help="Output directory")
    args = parser.parse_args()

    generate_returns(rows=args.rows, output_dir=Path(args.output))
