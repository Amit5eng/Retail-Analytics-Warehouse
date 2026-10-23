from pathlib import Path

import numpy as np
import pandas as pd


# 1. Project configuration

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"

SALES_DIR = RAW_DIR / "sales"

NUM_TRANSACTIONS = 10_000
NUM_PRODUCTS = 100
NUM_CUSTOMERS = 1_000
NUM_STORES = 10

RANDOM_SEED = 42

rng = np.random.default_rng(RANDOM_SEED)


# 2. Create output directories

SALES_DIR.mkdir(parents=True, exist_ok=True)


# 3. Generate product master data

categories = [
    "Electronics",
    "Clothing",
    "Home",
    "Groceries",
    "Sports",
]

products = pd.DataFrame(
    {
        "product_id": [
            f"P{i:04d}" for i in range(1, NUM_PRODUCTS + 1)
        ],
        "product_name": [
            f"Product_{i:03d}"
            for i in range(1, NUM_PRODUCTS + 1)
        ],
        "category": rng.choice(
            categories, size=NUM_PRODUCTS
        ),
        "unit_cost": np.round(
            rng.uniform(50, 5000, NUM_PRODUCTS), 2
        ),
        "unit_price": np.round(
            rng.uniform(100, 8000, NUM_PRODUCTS), 2
        ),
    }
)

# Ensure selling price exceeds cost.
products["unit_price"] = np.round(
    products["unit_cost"]
    + rng.uniform(20, 2000, NUM_PRODUCTS),
    2,
)

products.to_csv(
    RAW_DIR / "products.csv", index=False
)


# 4. Generate customer master data

cities = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Pune",
    "Chennai",
    "Kolkata",
    "Jaipur",
]

customers = pd.DataFrame(
    {
        "customer_id": [
            f"C{i:05d}" for i in range(1, NUM_CUSTOMERS + 1)
        ],
        "customer_name": [
            f"Customer_{i:04d}"
            for i in range(1, NUM_CUSTOMERS + 1)
        ],
        "city": rng.choice(
            cities, size=NUM_CUSTOMERS
        ),
        "signup_date": pd.to_datetime(
            rng.integers(
                pd.Timestamp("2023-01-01").value // 10**9,
                pd.Timestamp("2025-01-01").value // 10**9,
                NUM_CUSTOMERS,
            ),
            unit="s",
        ).strftime("%Y-%m-%d"),
    }
)

customers.to_csv(
    RAW_DIR / "customers.csv", index=False
)


# 5. Generate store master data

store_cities = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Pune",
    "Chennai",
    "Kolkata",
    "Jaipur",
    "Ahmedabad",
    "Lucknow",
]

stores = pd.DataFrame(
    {
        "store_id": [
            f"S{i:03d}" for i in range(1, NUM_STORES + 1)
        ],
        "store_name": [
            f"Store_{i:02d}"
            for i in range(1, NUM_STORES + 1)
        ],
        "city": store_cities,
        "region": [
            "West",
            "North",
            "South",
            "South",
            "West",
            "South",
            "East",
            "North",
            "West",
            "North",
        ],
    }
)

stores.to_csv(
    RAW_DIR / "stores.csv", index=False
)


# 6. Generate sales transactions

product_ids = products["product_id"].to_numpy()
customer_ids = customers["customer_id"].to_numpy()
store_ids = stores["store_id"].to_numpy()

product_prices = products.set_index(
    "product_id"
)["unit_price"]

product_costs = products.set_index(
    "product_id"
)["unit_cost"]

transaction_dates = pd.to_datetime(
    rng.integers(
        pd.Timestamp("2025-01-01").value // 10**9,
        pd.Timestamp("2026-01-01").value // 10**9,
        NUM_TRANSACTIONS,
    ),
    unit="s",
)

sales = pd.DataFrame(
    {
        "transaction_id": [
            f"T{i:07d}"
            for i in range(1, NUM_TRANSACTIONS + 1)
        ],
        "transaction_date": transaction_dates,
        "product_id": rng.choice(
            product_ids, size=NUM_TRANSACTIONS
        ),
        "customer_id": rng.choice(
            customer_ids, size=NUM_TRANSACTIONS
        ),
        "store_id": rng.choice(
            store_ids, size=NUM_TRANSACTIONS
        ),
        "quantity": rng.integers(
            1, 6, size=NUM_TRANSACTIONS
        ),
        "payment_method": rng.choice(
            ["Cash", "Card", "UPI", "NetBanking"],
            size=NUM_TRANSACTIONS,
        ),
        "order_status": rng.choice(
            ["Completed", "Completed", "Completed", "Returned"],
            size=NUM_TRANSACTIONS,
        ),
    }
)

sales["unit_price"] = sales["product_id"].map(
    product_prices
)

sales["unit_cost"] = sales["product_id"].map(
    product_costs
)

sales["sales_amount"] = (
    sales["quantity"] * sales["unit_price"]
).round(2)

sales["cost_amount"] = (
    sales["quantity"] * sales["unit_cost"]
).round(2)

sales["profit_amount"] = (
    sales["sales_amount"] - sales["cost_amount"]
).round(2)

sales["transaction_date"] = (
    sales["transaction_date"].dt.strftime("%Y-%m-%d")
)

# Save each month's transactions in a separate CSV.
sales["_month"] = sales["transaction_date"].str[:7]

for month, monthly_sales in sales.groupby("_month"):
    output_file = SALES_DIR / f"sales_{month}.csv"

    monthly_sales.drop(columns="_month").to_csv(
        output_file, index=False
    )


# 7. Print generation summary

print("\nRetail dataset generated successfully!")
print(f"Project directory: {PROJECT_ROOT}")
print(f"Total transactions: {len(sales):,}")
print(f"Unique products: {products['product_id'].nunique():,}")
print(f"Unique customers: {customers['customer_id'].nunique():,}")
print(f"Unique stores: {stores['store_id'].nunique():,}")
print(f"Monthly sales files: {sales['_month'].nunique()}")
print(f"Total sales amount: ₹{sales['sales_amount'].sum():,.2f}")
print(f"Total profit amount: ₹{sales['profit_amount'].sum():,.2f}")
print(f"Raw data directory: {RAW_DIR}")