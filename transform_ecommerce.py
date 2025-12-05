import pandas as pd
import numpy as np
import os

# ------------------------------------------
# PATHS
# ------------------------------------------
RAW_DIR = r"D:\DATA end to end projects\marketing etl\data\raw\brazilian-ecommerce"
STAGE_DIR = r"D:\DATA end to end projects\marketing etl\data\staged\ecommerce"

os.makedirs(STAGE_DIR, exist_ok=True)

# File paths
FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

# ------------------------------------------
# LOAD
# ------------------------------------------
def load_table(name):
    file_path = os.path.join(RAW_DIR, FILES[name])
    print(f"📥 Loading {name}: {file_path}")
    return pd.read_csv(file_path)

# ------------------------------------------
# CLEAN FUNCTIONS
# ------------------------------------------

def clean_orders(df):
    print("🧹 Cleaning orders...")
    date_cols = df.filter(like="date").columns
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def clean_order_items(df):
    print("🧹 Cleaning order_items...")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")
    return df

def clean_payments(df):
    print("🧹 Cleaning payments...")
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce")
    return df

def clean_products(df):
    print("🧹 Cleaning products...")
    for col in df.columns:
        if df[col].dtype == "object" and ("_cm" in col or "_g" in col):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def clean_customers(df):
    print("🧹 Cleaning customers...")
    return df  # minimal cleaning needed

def clean_geolocation(df):
    print("🧹 Cleaning geolocation...")
    df["geolocation_lat"] = pd.to_numeric(df["geolocation_lat"], errors="coerce")
    df["geolocation_lng"] = pd.to_numeric(df["geolocation_lng"], errors="coerce")
    return df

def clean_sellers(df):
    print("🧹 Cleaning sellers...")
    return df

def clean_reviews(df):
    print("🧹 Cleaning reviews...")
    df["review_creation_date"] = pd.to_datetime(df["review_creation_date"], errors="coerce")
    df["review_answer_timestamp"] = pd.to_datetime(df["review_answer_timestamp"], errors="coerce")
    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
    return df

def clean_category_translation(df):
    print("🧹 Cleaning category_translation...")
    return df

# Map table name → cleaning function
CLEAN_FUNCS = {
    "customers": clean_customers,
    "geolocation": clean_geolocation,
    "order_items": clean_order_items,
    "payments": clean_payments,
    "reviews": clean_reviews,
    "orders": clean_orders,
    "products": clean_products,
    "sellers": clean_sellers,
    "category_translation": clean_category_translation
}

# ------------------------------------------
# SAVE
# ------------------------------------------
def save_clean(df, name):
    csv_path = os.path.join(STAGE_DIR, f"{name}.csv")
    parquet_path = os.path.join(STAGE_DIR, f"{name}.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    print(f"💾 Saved {name}: {csv_path}")

# ------------------------------------------
# MAIN
# ------------------------------------------
def main():
    print("🚀 Transforming ALL Olist tables...\n")

    for name in FILES:
        df = load_table(name)
        df = CLEAN_FUNCS[name](df)
        save_clean(df, f"{name}_clean")

    print("\n🎉 ALL Olist datasets transformed successfully!")

if __name__ == "__main__":
    main()