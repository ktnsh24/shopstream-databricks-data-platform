"""Generate fake ShopStream customer data for local dev and lab exercises.

Usage:
    python data_generators/generate_customers.py --rows 1000 --output data_generators/output/
    python data_generators/generate_customers.py --rows 500 --upload --storage helixdataadls

What this does:
    Writes a realistic CSV file of ShopStream customers to disk.
    If --upload is set, also copies the CSV to ADLS Gen2 /raw/customers/
    so that the Auto Loader batch pipeline can pick it up.

Why this exists:
    In a real production environment, customers come from Lakeflow Connect
    reading ShopStream's PostgreSQL database. In a local dev or lab environment,
    we don't have that database — so we generate fake data to simulate the
    CSV fallback path (the Auto Loader in ingest_customers_batch.py).

    Think of it as: the refund management system dropped a CSV for you to test with.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path


REGIONS = [
    "nl-north", "nl-south", "nl-east", "nl-west", "nl-central",
    "be-flanders", "be-wallonia", "de-north", "de-south",
]
SEGMENTS = ["standard", "premium", "vip"]
SEGMENT_WEIGHTS = [0.6, 0.3, 0.1]  # most customers are standard

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Elijah", "Sophie", "Lucas",
    "Mia", "Mason", "Isabella", "Ethan", "Amelia", "Aiden", "Harper",
    "Lin", "Yuki", "Priya", "Mohammed", "Fatima", "Carlos", "Ana",
]
LAST_NAMES = [
    "de Vries", "Jansen", "van den Berg", "Bakker", "Visser", "Smit",
    "Muller", "Schmidt", "Fischer", "Weber", "Meyer", "Wagner",
    "Patel", "Kim", "Singh", "Tanaka", "Santos", "Garcia",
]


def _random_date(start: date, end: date) -> date:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_customers(rows: int, output_dir: Path) -> Path:
    """Generate a CSV file of fake customers."""
    output_dir.mkdir(parents=True, exist_ok=True)
    today = date.today()
    output_path = output_dir / f"customers_{today.strftime('%Y%m%d')}.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "customer_id", "first_name", "last_name", "email",
                "region", "segment", "registered_at", "date_of_birth", "is_active",
            ],
        )
        writer.writeheader()

        for i in range(1, rows + 1):
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            customer_id = f"C{i:06d}"
            registered = _random_date(date(2020, 1, 1), today)
            dob = _random_date(date(1960, 1, 1), date(2000, 1, 1))

            writer.writerow({
                "customer_id": customer_id,
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower().replace(' ', '')}_{i}@example.com",
                "region": random.choice(REGIONS),
                "segment": random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0],
                "registered_at": f"{registered}T00:00:00",
                "date_of_birth": str(dob),
                "is_active": random.choices(["true", "false"], weights=[0.9, 0.1], k=1)[0],
            })

    print(f"Generated {rows} customers → {output_path}")
    return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate ShopStream customer CSV")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate")
    parser.add_argument("--output", type=str, default="data_generators/output", help="Output directory")
    args = parser.parse_args()

    generate_customers(rows=args.rows, output_dir=Path(args.output))
