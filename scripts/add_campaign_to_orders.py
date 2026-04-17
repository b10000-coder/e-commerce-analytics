"""
One-time patch: adds campaign_id column to orders.csv.
After this patch, campaign_id is generated directly in generate.py.
"""
import csv, random, os
from datetime import datetime

random.seed(42)
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# read campaigns
camps = []
with open(os.path.join(RAW_DIR, "campaigns.csv")) as f:
    camps = list(csv.DictReader(f))

camp_windows = [
    (c["campaign_id"], c["start_date"], c["end_date"]) for c in camps
]

# read and patch orders
orders_path = os.path.join(RAW_DIR, "orders.csv")
with open(orders_path) as f:
    orders = list(csv.DictReader(f))

for o in orders:
    try:
        try:
            od = datetime.strptime(o["order_date"], "%Y-%m-%d").date()
        except:
            od = datetime.strptime(o["order_date"], "%d/%m/%Y").date()
    except:
        o["campaign_id"] = ""
        continue
    assigned = ""
    for cid, cs, ce in camp_windows:
        cs_d = datetime.strptime(cs, "%Y-%m-%d").date()
        ce_d = datetime.strptime(ce, "%Y-%m-%d").date()
        if cs_d <= od <= ce_d and random.random() < 0.30:
            assigned = cid
            break
    o["campaign_id"] = assigned

with open(orders_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=orders[0].keys())
    w.writeheader()
    w.writerows(orders)

print(f"Patched {len(orders)} orders with campaign_id")
assigned_count = sum(1 for o in orders if o["campaign_id"])
print(f"  {assigned_count} orders assigned to a campaign")
