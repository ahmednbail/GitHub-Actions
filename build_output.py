import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "dags"))

from transformations import clean_sales, aggregate_by_category
import pandas as pd

df = pd.read_csv("data/raw_sales.csv")
cleaned = clean_sales(df)
cleaned.to_csv("artifact/clean_sales.csv", index=False)
print(f"Cleaned: {len(cleaned)} rows")

agg = aggregate_by_category(cleaned)
agg.to_csv("artifact/sales_by_category.csv", index=False)
print(f"Aggregated: {len(agg)} categories")