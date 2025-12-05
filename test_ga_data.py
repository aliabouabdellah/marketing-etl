import pandas as pd

# Read the GA clean file
ga = pd.read_csv(r'D:\DATA end to end projects\marketing etl\data\staged\ga\ga_clean.csv')

print("=== GA DATA TEST ===\n")
print(f"Shape: {ga.shape}")
print(f"\nColumns: {ga.columns.tolist()}\n")

# Check the first 5 rows with all columns visible
print("First 5 rows (detailed view):")
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
print(ga.head(5))

print("\n\n=== CHECKING SPECIFIC COLUMNS ===")
print(f"\nSource column (first 10 values):\n{ga['source'].head(10).tolist()}")
print(f"\nMedium column (first 10 values):\n{ga['medium'].head(10).tolist()}")
print(f"\nVisits column (first 10 values):\n{ga['visits'].head(10).tolist()}")

print("\n\n=== FILE INFO ===")
print(f"File size: {ga.memory_usage(deep=True).sum() / 1024:.2f} KB")
print(f"\nData types:\n{ga.dtypes}")
