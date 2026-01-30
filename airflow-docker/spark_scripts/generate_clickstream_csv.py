import csv
import os
import random
from datetime import datetime, timedelta

from faker import Faker


def generate_clickstream_rows(n: int = 10, seed: int | None = 42):
    if seed is not None:
        random.seed(seed)

    faker = Faker()
    Faker.seed(seed)

    event_types = ["view", "add_to_cart", "purchase"]
    channels = ["Google Ads", "Facebook Ads", "Email", "Direct", "Organic Search", "Referral"]

    rows = []
    for _ in range(n):
        user_id = random.randint(1, 200)
        product_id = random.randint(1, 100)
        category_id = random.randint(1, 200)
        channel_id = random.randint(1, 8)

        event_type = random.choices(event_types, weights=[0.7, 0.2, 0.1], k=1)[0]
        ts = datetime.utcnow() - timedelta(minutes=random.randint(1, 60 * 24 * 7))

        price_viewed = round(random.uniform(10, 500), 2)
        discount_shown = round(random.uniform(0, 30), 2)
        is_new_user = random.random() < 0.2

        channel_name = random.choice(channels)

        rows.append(
            {
                "event_id": faker.random_int(1, 10_000_000),
                "user_id": user_id,
                "session_id": faker.uuid4(),
                "event_type": event_type,
                "product_id": product_id,
                "category_id": category_id,
                "channel_id": channel_id,
                "event_time": ts.isoformat(sep=" ", timespec="seconds"),
                "page_url": f"https://example.com/product/{product_id}",
                "referrer_url": f"https://{channel_name.lower().replace(' ', '')}.example.com",
                "campaign_name": f"{channel_name} {faker.word().title()}",
                "device_type": random.choice(["desktop", "mobile"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "os": random.choice(["Windows", "macOS", "Linux", "Android", "iOS"]),
                "country": faker.country(),
                "city": faker.city(),
                "price": price_viewed,
                "discount": discount_shown,
                "is_returning": str(not is_new_user),
                "is_read": "null",
            }
        )

    return rows


def main():
    output_dir = os.environ.get("CSV_OUT_DIR", "/opt/spark-data")
    os.makedirs(output_dir, exist_ok=True)

    out_path = os.path.join(output_dir, "clickstream_sample.csv")

    rows = generate_clickstream_rows(n=1000)
    fieldnames = [
        "event_id",
        "user_id",
        "session_id",
        "event_type",
        "product_id",
        "category_id",
        "channel_id",
        "event_time",
        "page_url",
        "referrer_url",
        "campaign_name",
        "device_type",
        "browser",
        "os",
        "country",
        "city",
        "price",
        "discount",
        "is_returning",
        "is_read",
    ]

    mode = "a" if os.path.exists(out_path) else "w"

    with open(out_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
