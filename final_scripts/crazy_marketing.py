import numpy as np
import pandas as pd
from pathlib import Path

# Base paths relative to project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_STAGED = PROJECT_ROOT / "data" / "staged"
ADS_PATH = DATA_STAGED / "ads" / "ads_campaign_performance_clean.csv"
GA_PATH = DATA_STAGED / "ga" / "ga_clean.csv"
ORDERS_PATH = DATA_STAGED / "ecommerce" / "orders_clean.csv"
ORDER_ITEMS_PATH = DATA_STAGED / "ecommerce" / "order_items_clean.csv"

OUTPUT_DIR = PROJECT_ROOT / "data" / "warehouse" / "marketing"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_inputs():
    ads = pd.read_csv(ADS_PATH, parse_dates=["date"])
    ga_raw = pd.read_csv(GA_PATH, parse_dates=["date"])
    orders = pd.read_csv(ORDERS_PATH, parse_dates=["order_purchase_timestamp"])
    order_items = pd.read_csv(ORDER_ITEMS_PATH)
    return ads, ga_raw, orders, order_items


def build_revenue_series(orders: pd.DataFrame, order_items: pd.DataFrame):
    revenue_per_order = (
        order_items.groupby("order_id")["price"]
        .sum()
        .reset_index()
        .rename(columns={"price": "revenue"})
    )

    order_metrics = (
        orders[["order_id", "order_purchase_timestamp"]]
        .merge(revenue_per_order, on="order_id", how="left")
    )

    order_metrics["date"] = pd.to_datetime(order_metrics["order_purchase_timestamp"].dt.date)

    daily_revenue = order_metrics.groupby("date")["revenue"].sum().reset_index()
    daily_tx = (
        order_metrics.groupby("date")["order_id"]
        .count()
        .reset_index()
        .rename(columns={"order_id": "transactions"})
    )
    return daily_revenue, daily_tx


def compute_marketing_table():
    ads, ga_raw, orders, order_items = load_inputs()

    ads["date"] = pd.to_datetime(ads["date"]).dt.normalize()

    ga = ga_raw.copy()
    ga["date"] = pd.to_datetime(ga["date"]).dt.normalize()

    daily_revenue, daily_tx = build_revenue_series(orders, order_items)

    merged_ads_ga = ads.merge(ga, on="date", how="left")

    marketing = (
        merged_ads_ga
        .merge(daily_revenue, on="date", how="left")
        .merge(daily_tx, on="date", how="left")
    )

    marketing.rename(
        columns={
            "revenue_y": "revenue",
            "transactions_y": "transactions",
        },
        inplace=True,
    )
    marketing.drop(columns=["revenue_x", "transactions_x"], inplace=True, errors="ignore")

    cols_to_drop = [
        "Click-Through Rate (CTR)",
        "Cost per Click (CPC)",
        "Conversion Rate (CVR)",
        "Cost per Conversion (CPA)",
        "Cost per Mille (CPM)",
        "ROAS",
    ]
    marketing.drop(columns=cols_to_drop, inplace=True, errors="ignore")

    marketing["CTR"] = marketing["clicks"] / marketing["impressions"].replace(0, np.nan)
    marketing["CPC"] = marketing["cost"] / marketing["clicks"].replace(0, np.nan)
    marketing["CPM"] = (marketing["cost"] / marketing["impressions"].replace(0, np.nan)) * 1000
    marketing["CVR"] = marketing["transactions"] / marketing["visits"].replace(0, np.nan)
    marketing["ROAS"] = marketing["revenue"] / marketing["cost"].replace(0, np.nan)
    marketing["CAC"] = marketing["cost"] / marketing["transactions"].replace(0, np.nan)
    marketing["profit"] = marketing["revenue"] - marketing["cost"]

    return marketing


def main():
    marketing = compute_marketing_table()
    output_path = OUTPUT_DIR / "fact_marketing_master.csv"
    marketing.to_csv(output_path, index=False)
    print(f"Marketing fact table created at {output_path}")


if __name__ == "__main__":
    main()
