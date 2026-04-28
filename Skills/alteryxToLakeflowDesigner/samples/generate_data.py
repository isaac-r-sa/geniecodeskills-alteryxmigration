"""Generate deterministic synthetic data for the Alteryx → Lakeflow Designer demo."""
import csv
import random
from pathlib import Path

random.seed(42)

OUT = Path(__file__).parent / "data"
OUT.mkdir(parents=True, exist_ok=True)

PRODUCTS = [
    (1,  "Organic Bananas",      "Produce",   2.49),
    (2,  "Whole Milk 1L",        "Dairy",     1.79),
    (3,  "Sourdough Loaf",       "Bakery",    3.99),
    (4,  "Free-Range Eggs 12pk", "Dairy",     4.49),
    (5,  "Pasta Sauce 500ml",    "Pantry",    2.29),
    (6,  "Ground Coffee 500g",   "Beverages", 8.99),
    (7,  "Greek Yogurt 1kg",     "Dairy",     5.49),
    (8,  "Olive Oil 1L",         "Pantry",    9.99),
    (9,  "Frozen Pizza",         "Frozen",    4.99),
    (10, "Apples 1kg",           "Produce",   3.49),
    (11, "Cheddar Cheese 250g",  "Dairy",     4.79),
    (12, "Dark Chocolate 100g",  "Snacks",    2.99),
]

POSITIVE = [
    "Absolutely loved it. Quality is top-notch and the price is fair.",
    "Great value for money, will buy again. My family enjoyed it.",
    "Fantastic product, fresh and delicious. Highly recommend.",
    "Excellent. Tastes like it should and the packaging was perfect.",
    "Really impressed. The flavour is amazing and it stays fresh longer.",
]
NEUTRAL = [
    "Decent product. Nothing remarkable but does the job.",
    "It's okay. Average quality at an average price.",
    "Tastes fine, packaging is standard. No complaints.",
    "Met expectations. Would consider buying again on sale.",
    "Average. Not the best I've had, not the worst either.",
]
NEGATIVE = [
    "Disappointed. The item arrived damaged and tasted stale.",
    "Poor quality, won't buy again. Felt overpriced for what it is.",
    "Not great. Texture was off and the smell was a bit unpleasant.",
    "Bad experience. Email me at angry.shopper@example.com to discuss.",
    "Terrible. Phone customer support on 0412-345-678 — they hung up.",
]
PII_TEMPLATES = [
    "Please contact me at jane.doe@example.com for follow-up.",
    "My phone is 0412-987-654 if you need to reach me.",
    "Refund requested by John Smith — reachable at 0498-111-222.",
]

def random_comment(rating):
    if rating >= 4:
        bank = POSITIVE
    elif rating == 3:
        bank = NEUTRAL
    else:
        bank = NEGATIVE
    base = random.choice(bank)
    if random.random() < 0.20:
        base = base + " " + random.choice(PII_TEMPLATES)
    return base

def random_date(start_year, end_year):
    y = random.randint(start_year, end_year)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y:04d}-{m:02d}-{d:02d}"

def write_feedback(path, n_rows, year_range, dup_some=False):
    rows = []
    for i in range(n_rows):
        cust = 1000 + random.randint(0, 49)
        prod = random.choice([p[0] for p in PRODUCTS])
        rating = random.choices([1, 2, 3, 4, 5], weights=[1, 1, 2, 3, 3])[0]
        comment = random_comment(rating)
        date = random_date(*year_range)
        rows.append([cust, prod, rating, comment, date])

    if dup_some:
        rows.append(rows[0])
        rows.append(rows[5])

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "product_id", "rating", "comment", "feedback_date"])
        w.writerows(rows)

def write_products(path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "product_name", "category", "price"])
        w.writerows(PRODUCTS)

write_feedback(OUT / "current_feedback.csv",    60, (2025, 2026), dup_some=True)
write_feedback(OUT / "historical_feedback.csv", 40, (2023, 2024))
write_products(OUT / "products.csv")

print(f"Wrote {OUT}/current_feedback.csv, historical_feedback.csv, products.csv")
