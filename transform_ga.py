import pandas as pd
import numpy as np
import os

# ------------------------------------------
# PATHS
# ------------------------------------------
RAW_GA = r"D:\DATA end to end projects\marketing etl\data\raw\ga\ga_transactions_20251128_122416.csv"
STAGE_GA_DIR = r"D:\DATA end to end projects\marketing etl\data\staged\ga"

os.makedirs(STAGE_GA_DIR, exist_ok=True)

# ------------------------------------------
# LOAD RAW GA DATA
# ------------------------------------------
def load_ga():
    print("📥 Loading GA raw data...")
    df = pd.read_csv(RAW_GA)
    print(f"   → {len(df):,} rows loaded")
    return df

# ------------------------------------------
# CLEAN GA DATA
# ------------------------------------------
def clean_ga(df):
    print("🧹 Cleaning GA data...")

    # Convert date to datetime - date is in YYYYMMDD integer format
    if "date" in df.columns:
        # Convert integer YYYYMMDD to string, then to datetime
        df["date"] = df["date"].astype(str)
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")

    # Common GA numeric fields
    numeric_cols = [
        "visits", "pageviews",
        "transactions", "revenue"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Normalize strings
    for col in ["source", "medium", "campaign", "deviceCategory"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    return df

# ------------------------------------------
# ADD GA KPIs
# ------------------------------------------
def add_ga_kpis(df):
    print("📊 Computing GA KPIs...")

    visits_non_zero = df["visits"].replace(0, np.nan)

    df["pages_per_visit"] = df["pageviews"] / visits_non_zero
    df["conversion_rate"] = df["transactions"] / visits_non_zero

    return df

# ------------------------------------------
# SAVE CLEANED GA
# ------------------------------------------
def save_ga(df):
    csv_path = os.path.join(STAGE_GA_DIR, "ga_clean.csv")
    parquet_path = os.path.join(STAGE_GA_DIR, "ga_clean.parquet")

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    print("💾 GA cleaned data saved:")
    print(f"   CSV: {csv_path}")
    print(f"   Parquet: {parquet_path}")

# ------------------------------------------
# MAIN
# ------------------------------------------
def main():
    print("🚀 Starting GA transformation...\n")

    df = load_ga()
    df = clean_ga(df)
    df = add_ga_kpis(df)
    save_ga(df)

    print("\n🎉 Google Analytics transformation completed!")


if __name__ == "__main__":
    main()
