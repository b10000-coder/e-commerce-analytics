"""
Day 1 — Trendyol-style e-commerce data generator.

Produces 6 CSV files that land in data/raw/ as immutable source tables:
  customers.csv, sellers.csv, products.csv,
  orders.csv, order_items.csv, sessions.csv

Intentional messiness (documented so you can narrate it in the interview):
  - ~3% duplicate customer rows (same person, re-registered)
  - ~5% of orders reference a customer_id that doesn't exist
  - Null city on ~8% of customers
  - Two date formats in orders.csv (ISO + dd/mm/yyyy) to force casting in staging
  - Unit prices occasionally differ from the product's listed price (promo/flash sales)
  - Some sessions have null customer_id (anonymous browsing)
"""

import csv
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("tr_TR")       # Turkish locale for realistic names/cities
fake_en = Faker("en_US")    # English for product names
random.seed(42)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# ── date range: Oct 2024 – Mar 2025 (6 months) ─────────────────────────────
START_DATE = datetime(2024, 10, 1)
END_DATE   = datetime(2025, 3, 31)

def rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def iso(dt):       return dt.strftime("%Y-%m-%d")
def euro(dt):      return dt.strftime("%d/%m/%Y")          # intentional bad format
def maybe_null(v, pct=0.08):
    return "" if random.random() < pct else v


# ── CONFIG ──────────────────────────────────────────────────────────────────
N_CUSTOMERS   = 600
N_SELLERS     = 80
N_PRODUCTS    = 400
N_ORDERS      = 2500
N_SESSIONS    = 4000

TURKISH_CITIES = [
    "Istanbul", "Ankara", "Izmir", "Bursa", "Antalya",
    "Adana", "Konya", "Gaziantep", "Mersin", "Kayseri",
    "Eskisehir", "Trabzon", "Diyarbakir", "Samsun", "Denizli",
]

CATEGORIES = {
    "Electronics":  ["Smartphone", "Laptop", "Tablet", "Headphones", "Smartwatch", "Charger", "Speaker"],
    "Fashion":      ["T-Shirt", "Dress", "Jeans", "Jacket", "Sneakers", "Boots", "Bag", "Scarf"],
    "Home & Life":  ["Pillow", "Duvet", "Cookware Set", "Coffee Maker", "Vacuum Cleaner", "Lamp", "Mirror"],
    "Beauty":       ["Moisturizer", "Serum", "Lipstick", "Perfume", "Shampoo", "Face Mask"],
    "Sports":       ["Running Shoes", "Yoga Mat", "Resistance Band", "Protein Powder", "Bicycle Helmet"],
    "Books":        ["Novel", "Cookbook", "Self-Help Book", "Science Book", "Children's Book"],
    "Toys":         ["Board Game", "Puzzle", "Action Figure", "Building Blocks", "Doll"],
}

PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "cash_on_delivery", "wallet"]
ORDER_STATUSES  = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
STATUS_WEIGHTS  = [0.05, 0.10, 0.15, 0.55, 0.10, 0.05]
PLATFORMS       = ["ios", "android", "web"]
DEVICES         = ["mobile", "mobile", "mobile", "desktop", "tablet"]   # mobile-heavy like Trendyol


# ── HELPER ───────────────────────────────────────────────────────────────────
def write_csv(rows, filename):
    if not rows:
        return
    path = os.path.join(RAW_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# ── 1. SELLERS ───────────────────────────────────────────────────────────────
sellers = []
for i in range(1, N_SELLERS + 1):
    category = random.choice(list(CATEGORIES.keys()))
    sellers.append({
        "seller_id":    f"S{i:04d}",
        "seller_name":  fake.company(),
        "category":     category,
        "city":         random.choice(TURKISH_CITIES),
        "is_verified":  random.choice([True, True, True, False]),    # 75% verified
        "joined_date":  iso(rand_date(datetime(2020, 1, 1), START_DATE)),
        "rating":       round(random.uniform(3.0, 5.0), 1),
        "total_reviews": random.randint(10, 50000),
    })

write_csv(sellers, "sellers.csv")
print(f"  sellers:    {len(sellers):,} rows")


# ── 2. PRODUCTS ──────────────────────────────────────────────────────────────
products = []
for i in range(1, N_PRODUCTS + 1):
    category    = random.choice(list(CATEGORIES.keys()))
    subcategory = random.choice(CATEGORIES[category])
    cost        = round(random.uniform(10, 800), 2)
    price       = round(cost * random.uniform(1.15, 2.5), 2)
    seller      = random.choice(sellers)
    products.append({
        "product_id":    f"P{i:05d}",
        "product_name":  f"{fake_en.color_name()} {subcategory}",
        "category":      category,
        "subcategory":   subcategory,
        "seller_id":     seller["seller_id"],
        "list_price":    price,
        "cost_price":    cost,
        "is_active":     random.choice([True, True, True, True, False]),   # 80% active
        "stock_qty":     random.randint(0, 500),
        "created_at":    iso(rand_date(datetime(2022, 1, 1), START_DATE)),
    })

write_csv(products, "products.csv")
print(f"  products:   {len(products):,} rows")


# ── 3. CUSTOMERS (with ~3% duplicates) ───────────────────────────────────────
customers = []
customer_ids = []
for i in range(1, N_CUSTOMERS + 1):
    signup = rand_date(datetime(2020, 1, 1), END_DATE)
    tier   = random.choices(
        ["bronze", "silver", "gold"],
        weights=[0.60, 0.28, 0.12]
    )[0]
    cid = f"C{i:05d}"
    customer_ids.append(cid)
    customers.append({
        "customer_id":   cid,
        "first_name":    fake.first_name(),
        "last_name":     fake.last_name(),
        "email":         fake_en.email(),
        "phone":         fake.phone_number(),
        "city":          maybe_null(random.choice(TURKISH_CITIES), pct=0.08),
        "country":       "TR",
        "customer_tier": tier,
        "signup_date":   iso(signup),
        "is_deleted":    random.random() < 0.02,     # 2% soft-deleted
    })

# inject ~3% duplicates (same person re-registered with slightly different email)
n_dupes = int(N_CUSTOMERS * 0.03)
for _ in range(n_dupes):
    original = random.choice(customers)
    dupe = original.copy()
    dupe["customer_id"] = f"C{random.randint(99000, 99999)}"
    dupe["email"]       = fake_en.email()            # different email, same person
    customers.append(dupe)

random.shuffle(customers)
write_csv(customers, "customers.csv")
print(f"  customers:  {len(customers):,} rows  ({n_dupes} dupes injected)")


# ── 4. ORDERS (with two date formats + ~5% bad customer_ids) ─────────────────
# build a dict for fast lookup
valid_customer_ids = customer_ids   # original IDs only (not dupes)

# pre-build campaign ids for assignment to orders
_campaign_ids = [f"CAM{i:03d}" for i in range(1, 21)]

orders = []
order_ids = []
for i in range(1, N_ORDERS + 1):
    oid = f"O{i:06d}"
    order_ids.append(oid)
    order_date = rand_date()

    # ~5% reference a non-existent customer (referential integrity break)
    if random.random() < 0.05:
        cust_id = f"C{random.randint(90000, 90999)}"   # bogus
    else:
        cust_id = random.choice(valid_customer_ids)

    # ~40% use European date format (intentional mess)
    if random.random() < 0.40:
        date_str = euro(order_date)
    else:
        date_str = iso(order_date)

    discount = round(random.uniform(0, 50), 2) if random.random() < 0.35 else 0.0
    status   = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]

    orders.append({
        "order_id":        oid,
        "customer_id":     cust_id,
        "order_date":      date_str,           # mixed formats!
        "order_status":    status,
        "payment_method":  random.choice(PAYMENT_METHODS),
        "shipping_city":   random.choice(TURKISH_CITIES),
        "discount_amount": discount,
        "coupon_code":     maybe_null(f"COUP{random.randint(100,999)}", pct=0.70),
        "platform":        random.choice(PLATFORMS),
        "campaign_id":     random.choice(_campaign_ids) if random.random() < 0.60 else "",
    })

write_csv(orders, "orders.csv")
print(f"  orders:     {len(orders):,} rows  (40% euro dates, 5% bad customer_ids)")


# ── 5. ORDER_ITEMS ───────────────────────────────────────────────────────────
order_items = []
item_counter = 1
for order in orders:
    n_items = random.choices([1, 2, 3, 4, 5], weights=[0.45, 0.30, 0.14, 0.07, 0.04])[0]
    chosen_products = random.sample(products, min(n_items, len(products)))
    for prod in chosen_products:
        # unit price occasionally differs (flash sale / negotiated price)
        list_price = prod["list_price"]
        if random.random() < 0.20:
            unit_price = round(list_price * random.uniform(0.60, 0.95), 2)   # flash sale
        else:
            unit_price = list_price

        order_items.append({
            "order_item_id":  f"OI{item_counter:07d}",
            "order_id":       order["order_id"],
            "product_id":     prod["product_id"],
            "seller_id":      prod["seller_id"],
            "quantity":       random.randint(1, 4),
            "unit_price":     unit_price,
            "list_price":     list_price,
            "discount_pct":   round(random.uniform(0, 0.30), 3) if random.random() < 0.25 else 0.0,
        })
        item_counter += 1

write_csv(order_items, "order_items.csv")
print(f"  order_items:{len(order_items):,} rows")


# ── 6. SESSIONS ──────────────────────────────────────────────────────────────
sessions = []
for i in range(1, N_SESSIONS + 1):
    session_date = rand_date()
    # ~20% anonymous sessions
    cust_id = maybe_null(random.choice(valid_customer_ids), pct=0.20)
    duration = random.randint(10, 3600)
    page_views = max(1, int(duration / random.uniform(20, 120)))
    is_converted = random.random() < 0.12   # 12% conversion rate

    sessions.append({
        "session_id":       f"SS{i:07d}",
        "customer_id":      cust_id,
        "session_date":     iso(session_date),
        "platform":         random.choice(PLATFORMS),
        "device_type":      random.choice(DEVICES),
        "page_views":       page_views,
        "duration_seconds": duration,
        "is_converted":     is_converted,
        "utm_source":       maybe_null(
            random.choice(["google", "facebook", "instagram", "email", "direct", "affiliate"]),
            pct=0.30
        ),
    })

write_csv(sessions, "sessions.csv")
print(f"  sessions:   {len(sessions):,} rows")


print("\nDone. Files written to data/raw/")
print(f"Date range: {iso(START_DATE)} → {iso(END_DATE)}")

# ── NEW: CAMPAIGNS ────────────────────────────────────────────────────────────
def generate_campaigns(n=20, start_date=datetime(2024,10,1), end_date=datetime(2025,3,31)):
    import random
    CAMPAIGN_TYPES = ["flash_sale", "coupon", "seasonal", "loyalty"]
    rows = []
    for i in range(1, n+1):
        cs = start_date + timedelta(days=random.randint(0, (end_date-start_date).days - 10))
        ce = cs + timedelta(days=random.randint(1, 7))
        if ce > end_date:
            ce = end_date
        rows.append({
            "campaign_id":   f"CAM{i:03d}",
            "campaign_name": f"Campaign {i}",
            "campaign_type": random.choice(CAMPAIGN_TYPES),
            "start_date":    cs.strftime("%Y-%m-%d"),
            "end_date":      ce.strftime("%Y-%m-%d"),
            "discount_pct":  round(random.uniform(0.05, 0.50), 2),
            "is_active":     "true",
        })
    return rows


# ── NEW: SELLER CHANGES ───────────────────────────────────────────────────────
def generate_seller_changes(seller_ids, n_sellers=60,
                             start_date=datetime(2024,10,1),
                             end_date=datetime(2025,3,31)):
    SELLER_TIERS = ["standard", "premium", "elite"]
    chosen = random.sample(seller_ids, min(n_sellers, len(seller_ids)))
    rows = []
    for sid in chosen:
        n_events = random.randint(1, 2)
        used = set()
        for _ in range(n_events):
            d = start_date + timedelta(days=random.randint(0, (end_date-start_date).days))
            while d in used:
                d = start_date + timedelta(days=random.randint(0, (end_date-start_date).days))
            used.add(d)
            rows.append({
                "seller_id":   sid,
                "seller_tier": random.choice(SELLER_TIERS),
                "rating":      round(random.uniform(3.0, 5.0), 2),
                "change_date": d.strftime("%Y-%m-%d"),
            })
    rows.sort(key=lambda r: (r["seller_id"], r["change_date"]))
    return rows


# ── NEW: SELLER DAILY STATS ───────────────────────────────────────────────────
def generate_seller_daily_stats(order_items_rows, orders_rows, sellers_rows):
    from collections import defaultdict
    order_map = {o["order_id"]: o for o in orders_rows}
    daily = defaultdict(lambda: {
        "gmv": 0.0, "total_orders": set(),
        "cancelled": set(), "returned": set(), "total_items": 0
    })
    for oi in order_items_rows:
        order = order_map.get(oi["order_id"])
        if not order:
            continue
        try:
            # parse either date format
            od = oi.get("order_date") or order.get("order_date", "")
            try:
                d = datetime.strptime(od, "%Y-%m-%d").strftime("%Y-%m-%d")
            except:
                d = datetime.strptime(od, "%d/%m/%Y").strftime("%Y-%m-%d")
        except:
            continue
        key = (oi["seller_id"], d)
        qty  = int(oi.get("quantity", 1))
        up   = float(oi.get("unit_price", 0))
        dp   = float(oi.get("discount_pct", 0) or 0)
        rev  = round(up * qty * (1 - dp), 2)
        daily[key]["gmv"] += rev
        daily[key]["total_orders"].add(oi["order_id"])
        daily[key]["total_items"] += 1
        status = order.get("order_status", "")
        if status == "cancelled":
            daily[key]["cancelled"].add(oi["order_id"])
        if status == "returned":
            daily[key]["returned"].add(oi["order_id"])

    rows = []
    for (seller_id, stat_date), v in daily.items():
        total = len(v["total_orders"])
        rows.append({
            "seller_id":          seller_id,
            "stat_date":          stat_date,
            "gmv":                round(v["gmv"], 2),
            "total_orders":       total,
            "cancelled_orders":   len(v["cancelled"]),
            "returned_orders":    len(v["returned"]),
            "total_items":        v["total_items"],
            "cancellation_rate":  round(len(v["cancelled"]) / total, 4) if total else 0,
            "return_rate":        round(len(v["returned"]) / total, 4) if total else 0,
            "avg_rating":         round(random.uniform(3.0, 5.0), 2),
        })
    rows.sort(key=lambda r: (r["seller_id"], r["stat_date"]))
    return rows


# ── NEW: WRITE HELPER ─────────────────────────────────────────────────────────
def write_csv_extra(filename, rows):
    if not rows:
        print(f"SKIP {filename} — no rows")
        return
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    path = os.path.join(out_dir, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"OK   {filename:<35} {len(rows):>5} rows")

# ── CALL NEW GENERATORS ───────────────────────────────────────────────────────
campaigns = generate_campaigns(20)
write_csv_extra("campaigns.csv", campaigns)

seller_ids = [s["seller_id"] for s in sellers]
seller_changes = generate_seller_changes(seller_ids, n_sellers=60)
write_csv_extra("seller_changes.csv", seller_changes)

seller_daily = generate_seller_daily_stats(order_items, orders, sellers)
write_csv_extra("seller_daily_stats.csv", seller_daily)
