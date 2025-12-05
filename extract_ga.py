import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
CREDENTIALS_PATH = "credentials/bigquery_service_key.json"
OUTPUT_DIR = "data/raw/ga"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------
# AUTHENTICATION
# -----------------------------
print("🔐 Authenticating to BigQuery...")
client = bigquery.Client.from_service_account_json(CREDENTIALS_PATH)
print("✅ Authenticated successfully.\n")

# -----------------------------
# QUERY (Your validated query)
# -----------------------------
QUERY = """
SELECT
  fullVisitorId,
  visitId,
  date,
  trafficSource.source AS source,
  trafficSource.medium AS medium,
  totals.visits AS visits,
  totals.pageviews AS pageviews,
  totals.transactions AS transactions,
  totals.transactionRevenue/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE totals.transactions IS NOT NULL
  AND totals.transactions > 0
ORDER BY date
"""

print("▶ Running query...")
job = client.query(QUERY)
df = job.to_dataframe()
print(f"📊 Query returned {len(df):,} rows.\n")

# -----------------------------
# SAVE FILES
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

csv_path = os.path.join(OUTPUT_DIR, f"ga_transactions_{timestamp}.csv")
parquet_path = os.path.join(OUTPUT_DIR, f"ga_transactions_{timestamp}.parquet")

df.to_csv(csv_path, index=False)
df.to_parquet(parquet_path, index=False)

print(f"💾 Saved CSV to: {csv_path}")
print(f"💾 Saved Parquet to: {parquet_path}\n")

print("🎉 Google Analytics extraction completed successfully.")
