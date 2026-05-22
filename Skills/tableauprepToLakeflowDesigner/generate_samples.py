#!/usr/bin/env python3
"""Generate sample CSV test data for the converted Tableau Prep → Visual Data Prep
notebooks in this folder.

Sources covered (mirrors the `extracted_data/` directory layout the notebooks
expect):

  Main FY Capacity flow (MAIN_FyForecastCapacityFlow_EDLAP.designer.ipynb)
  ───────────────────────────────────────────────────────────────────────
  extracted_data/fyforecast.csv
  extracted_data/synthetic/division.csv
  extracted_data/synthetic/supplier.csv
  extracted_data/synthetic/product.csv
  extracted_data/synthetic/contract.csv
  extracted_data/synthetic/ahead_to_cbis_code.csv
  extracted_data/synthetic/2025_items_1.csv
  extracted_data/synthetic/apt.csv
  extracted_data/synthetic/sourcing_timeline.csv
  extracted_data/synthetic/custom_sql.csv

  Test pipelines (tests/{simple,complex,highly_complex}.designer.ipynb)
  ───────────────────────────────────────────────────────────────────────
  extracted_data/orders.csv
  extracted_data/customers.csv
  extracted_data/inventory.csv
  extracted_data/synthetic/customers.csv
  extracted_data/synthetic/products.csv
  extracted_data/synthetic/capacity.csv

Run:
  python3 generate_samples.py
  # → writes everything under ./samples/extracted_data/

Upload to workspace afterwards (so the notebooks find the files):
  databricks workspace import-dir samples/extracted_data \
    "/Workspace/Users/<you>/tableau dashboard/extracted_data" \
    --overwrite -p <profile>
"""
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

OUT = Path(__file__).parent / "samples" / "extracted_data"
SYN = OUT / "synthetic"
OUT.mkdir(parents=True, exist_ok=True)
SYN.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {path.relative_to(OUT.parent.parent)}  ({len(rows)} rows)")


# ───────────────────────────────────────────────────────────────────────
# Reference universe — kept in sync across all FY-capacity files
# ───────────────────────────────────────────────────────────────────────
DIVISIONS = list(range(1, 21)) + [997, 998, 999]   # include regional pseudo-suppliers

SUPPLIERS = [
    (10001, "Acme Foods Co"),
    (10002, "Heritage Bakers Ltd"),
    (10003, "Coastal Beverages"),
    (10004, "Northern Dairy Group"),
    (10005, "Sunrise Produce"),
    (10006, "Greenfield Organics"),
    (10007, "Pacific Snacks"),
    (10008, "Highland Meats"),
    (10009, "Crystal Mills"),
    (10010, "Alpine Cheese Works"),
    (10011, "Riverbend Cellars"),       # alcohol
    (10012, "Sunset Brewing Co"),       # alcohol
    (10013, "Vineyard Estates"),        # alcohol
    (10014, "Goldwheat Bakery"),
    (10015, "Sweetwater Ranch"),
    (10016, "Mountain Spring Foods"),
    (10017, "Ocean Catch Seafoods"),
    (10018, "Prairie Grain Mill"),
    (10019, "Sugarcane Confectioners"),
    (10020, "Frontier Frozen Foods"),
]

CG_CATEGORIES = [
    (1,  "Bakery",          "non_seasonal"),
    (2,  "Sparkling wine",  "non_seasonal"),     # alcohol
    (3,  "Wine",            "non_seasonal"),     # alcohol
    (4,  "Beer",            "non_seasonal"),     # alcohol
    (5,  "Dairy",           "non_seasonal"),
    (6,  "Produce",         "Seasonal"),
    (7,  "Snacks",          "non_seasonal"),
    (8,  "Frozen",          "non_seasonal"),
    (9,  "Meat",            "non_seasonal"),
    (10, "Confectionery",   "non_seasonal"),
    (11, "Beverage",        "non_seasonal"),
    (12, "Seafood",         "non_seasonal"),
]

BUYING_DIRECTORS = [
    "Anita Gomez", "Brett Cho", "Carolyn Patel", "Derek Lin", "Emily Faraday",
    "Felipe Ortega", "Grace Wu", "Hassan Mehta", "Iris Tanaka", "Jamal Reed",
]

# Build a product universe of 200 SKUs spread across CGs/suppliers
def build_products():
    products = []
    pid = 100000
    for _ in range(200):
        cg, cgdesc, prodclass = random.choice(CG_CATEGORIES)
        scg = random.randint(1, 5)
        scgdesc = f"{cgdesc} subgroup {scg}"
        # Alcohol always tied to alcohol suppliers
        if cg in (2, 3, 4):
            sup = random.choice([10011, 10012, 10013])
        else:
            sup = random.choice([s[0] for s in SUPPLIERS if s[0] not in (10011, 10012, 10013)])
        retail = round(random.uniform(0.99, 49.99), 2)
        packsize = random.choice([6, 8, 12, 18, 24, 36])
        bd = random.choice(BUYING_DIRECTORS)
        gbd = random.choice(BUYING_DIRECTORS)
        products.append({
            "productCode":         pid,
            "mainCode":            pid,                     # Con_ProductCode same as ProductCode for simplicity
            "supplierNo":          sup,
            "description":         f"{cgdesc} item {pid}",
            "CGNo":                cg,
            "cgDesc":              cgdesc,
            "cgComplete":          f"{cg:02d} - {cgdesc}",
            "SCGNo":               scg,
            "scgDesc":             scgdesc,
            "scgComplete":         f"{scg:02d} - {scgdesc}",
            "productClass":        prodclass,
            "buyingDirector":      bd,
            "groupBuyingDirector": gbd,
            "packsize":            packsize,
            "retail":              retail,
        })
        pid += 1
    return products


PRODUCTS = build_products()


# ───────────────────────────────────────────────────────────────────────
# FY-capacity files
# ───────────────────────────────────────────────────────────────────────
print("FY-capacity flow files:")

# division.csv
write_csv(
    SYN / "division.csv",
    ["divNo", "divId", "sortByDivOrder"],
    [[d, f"DIV{d:04d}", i] for i, d in enumerate(DIVISIONS, start=1)],
)

# supplier.csv
write_csv(
    SYN / "supplier.csv",
    ["supplierNo", "supplier"],
    [[s[0], s[1]] for s in SUPPLIERS],
)

# product.csv
write_csv(
    SYN / "product.csv",
    ["productCode", "mainCode", "description", "CGNo", "retail",
     "cgDesc", "cgComplete", "SCGNo", "scgDesc", "scgComplete",
     "productClass", "buyingDirector", "groupBuyingDirector", "packsize"],
    [[p["productCode"], p["mainCode"], p["description"], p["CGNo"], p["retail"],
      p["cgDesc"], p["cgComplete"], p["SCGNo"], p["scgDesc"], p["scgComplete"],
      p["productClass"], p["buyingDirector"], p["groupBuyingDirector"], p["packsize"]]
     for p in PRODUCTS],
)

# contract.csv — a contract per (productCode, supplierNo) for ~80% of products
contract_rows = []
for p in PRODUCTS:
    if random.random() < 0.8:
        contract_rows.append([
            p["productCode"], p["supplierNo"],
            p["packsize"], random.choice([60, 72, 96, 108, 120]),
        ])
write_csv(
    SYN / "contract.csv",
    ["productCode", "supplierNo", "packsize", "casesPerPallet"],
    contract_rows,
)

# ahead_to_cbis_code.csv — SAP ↔ CBIS crosswalk for ~70% of products
ahead_rows = []
sap = 800000
for p in PRODUCTS:
    if random.random() < 0.7:
        ahead_rows.append([sap, p["productCode"], 1])
        sap += 1
write_csv(
    SYN / "ahead_to_cbis_code.csv",
    ["sapProductKey", "cbisProductKey", "active"],
    ahead_rows,
)

# fyforecast.csv — multi-year forecast per (product × supplier × division)
forecast_rows = []
this_year = date.today().year
for p in PRODUCTS:
    # 2–4 divisions per product, occasionally regional (997-999)
    n_divs = random.randint(2, 4)
    pool = DIVISIONS.copy()
    if random.random() < 0.15:
        pool = pool + [random.choice([997, 998, 999])]
    for divNo in random.sample(pool, k=min(n_divs, len(pool))):
        # baseline cases scaled by retail/packsize
        base = max(50, int(2000 / max(p["retail"], 1)))
        fc = [round(base * random.uniform(0.85, 1.20), 1) for _ in range(6)]
        forecast_rows.append([
            divNo, p["mainCode"], p["productCode"],
            round(sum(fc), 1),
            random.randint(50, 300),
            this_year + 0, this_year + 1, this_year + 2,
            this_year + 3, this_year + 4, this_year + 5,
            *fc,
            p["supplierNo"],
            int(base * random.uniform(0.7, 1.3) * p["packsize"]),
        ])
write_csv(
    OUT / "fyforecast.csv",
    ["divNo", "mainCode", "productCode", "sumOfCases", "startingStoreCount",
     "year1", "year2", "year3", "year4", "year5", "year6",
     "fcYear1", "fcYear2", "fcYear3", "fcYear4", "fcYear5", "fcYear6",
     "supplierNo", "lySales"],
    forecast_rows,
)

# 2025_items_1.csv — SST/VCO team designation
# NOTE: original file has column "Item  Code" with TWO spaces (preserved verbatim).
team_rows = []
for p in random.sample(PRODUCTS, k=int(len(PRODUCTS) * 0.6)):
    for yr in (2027, 2028, 2029, 2030, 2031):
        team_rows.append([
            p["productCode"], p["description"],
            p["cgComplete"], p["scgComplete"],
            yr, random.choice(["SST", "VCO", "SST", ""]),
        ])
write_csv(
    SYN / "2025_items_1.csv",
    ["Item  Code", "Description", "CG", "SCG", "Year", "Team"],
    team_rows,
)

# apt.csv — items currently in test (for the anti-join filter)
apt_rows = []
testing_pool = random.sample(PRODUCTS, k=20)
for i, p in enumerate(testing_pool, start=1):
    apt_rows.append([
        i,
        f"Quality test {i:03d}",
        random.choice(["Anita Gomez", "Brett Cho", "Derek Lin"]),
        random.choice(["1 - On Track", "2 - Delayed", "5 - Delivered",
                       "3 - Cancelled", "4 - Hold"]),
        str(p["productCode"]),
        str(this_year + random.randint(0, 3)),
        str(p["CGNo"]),
    ])
write_csv(
    SYN / "apt.csv",
    ["Unique ID", "Test Name", "Assigned Analyst", "Status",
     "Legacy Code", "Year", "CG No"],
    apt_rows,
)

# sourcing_timeline.csv — placeholder (used as upstream input in the flow but
# only spot-checked downstream). Keep it lightweight.
sourcing_rows = []
for p in random.sample(PRODUCTS, k=80):
    sourcing_rows.append([
        p["productCode"], p["description"], p["cgComplete"],
        random.choice(["SST", "VCO"]),
        (date.today() + timedelta(days=random.randint(-90, 365))).isoformat(),
        random.choice(["scoping", "in flight", "launched", "blocked"]),
    ])
write_csv(
    SYN / "sourcing_timeline.csv",
    ["productCode", "description", "cgComplete", "team", "milestone_date", "status"],
    sourcing_rows,
)

# custom_sql.csv — the "Custom SQL" Tableau Prep node, simulated
custom_sql_rows = []
for p in random.sample(PRODUCTS, k=120):
    custom_sql_rows.append([
        p["productCode"], p["supplierNo"], p["CGNo"],
        round(random.uniform(0.05, 0.45), 4),
        random.choice(["A", "B", "C"]),
    ])
write_csv(
    SYN / "custom_sql.csv",
    ["productCode", "supplierNo", "CGNo", "margin_rate", "tier"],
    custom_sql_rows,
)


# ───────────────────────────────────────────────────────────────────────
# Test-pipeline files
# ───────────────────────────────────────────────────────────────────────
print("\nTest pipeline files:")

# orders.csv
N_CUSTOMERS = 50
N_PRODUCTS  = 30
order_rows = []
for oid in range(1, 501):
    order_rows.append([
        oid,
        random.randint(1, N_CUSTOMERS),
        round(random.uniform(5.0, 500.0), 2),
        random.choice(["new", "shipped", "delivered", "returned", "cancelled"]),
        (date.today() - timedelta(days=random.randint(0, 365))).isoformat(),
    ])
write_csv(
    OUT / "orders.csv",
    ["order_id", "customer_id", "amount", "status", "placed_at"],
    order_rows,
)

# customers.csv (used by `complex` from extracted_data/)
customer_rows = []
regions = ["North", "South", "East", "West", "Central"]
first  = ["Alex", "Bao", "Cleo", "Dilan", "Elena", "Farah", "Gus", "Hana",
          "Ivo", "Jules", "Karim", "Lila", "Mira", "Nico", "Omar"]
last   = ["Patel", "Wong", "Garcia", "Kim", "Smith", "Liu", "Brown", "Diaz",
          "Singh", "Mendez", "Cohen", "Rossi", "Park", "Hill"]
for cid in range(1, N_CUSTOMERS + 1):
    customer_rows.append([
        cid,
        f"{random.choice(first)} {random.choice(last)}",
        random.choice(regions),
        random.randint(2018, 2025),
    ])
write_csv(
    OUT / "customers.csv",
    ["customer_id", "name", "region", "signup_year"],
    customer_rows,
)

# customers.csv (synthetic copy used by `highly_complex`)
write_csv(
    SYN / "customers.csv",
    ["customer_id", "name", "region", "signup_year"],
    customer_rows,
)

# products.csv
product_rows = []
categories = ["Apparel", "Electronics", "Home", "Outdoor", "Beauty", "Pantry"]
for pid in range(1, N_PRODUCTS + 1):
    product_rows.append([
        pid,
        f"SKU-{pid:04d}",
        random.choice(categories),
        round(random.uniform(2.99, 199.99), 2),
    ])
write_csv(
    SYN / "products.csv",
    ["product_id", "sku", "category", "unit_price"],
    product_rows,
)

# inventory.csv (highly_complex)
inv_rows = []
locations = ["Warehouse-A", "Warehouse-B", "Store-N", "Store-S"]
for pid in range(1, N_PRODUCTS + 1):
    for loc in locations:
        inv_rows.append([
            pid, loc,
            random.randint(0, 500),
            random.choice([20, 50, 100]),
        ])
write_csv(
    OUT / "inventory.csv",
    ["product_id", "location", "on_hand", "reorder_level"],
    inv_rows,
)

# capacity.csv (highly_complex — referenced columns: cap_year, year_2027..year_2031,
# capacity, Region). Wide format with one row per (Region × supplier).
cap_rows = []
for sup, name in SUPPLIERS:
    for region in ["North", "South", "East", "West"]:
        cap_rows.append([
            sup, name, region,
            random.randint(2027, 2031),
            random.randint(1000, 50000),
            random.randint(1000, 50000),
            random.randint(1000, 50000),
            random.randint(1000, 50000),
            random.randint(1000, 50000),
            random.randint(1000, 50000),
        ])
write_csv(
    SYN / "capacity.csv",
    ["supplier_id", "supplier_name", "Region", "cap_year",
     "year_2027", "year_2028", "year_2029", "year_2030", "year_2031", "capacity"],
    cap_rows,
)

print(f"\nDone. Files written under: {OUT}")
print("\nUpload to workspace with:")
print(f"  databricks workspace import-dir {OUT} "
      f'"/Workspace/Users/<you>/tableau dashboard/extracted_data" '
      f"--overwrite -p <profile>")
