
from __future__ import annotations

import pandas as pd

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.dropna(subset=["customer_id", "product_category", "revenue"])
    cleaned = cleaned[cleaned["revenue"] >= 0]
    return cleaned.reset_index(drop=True)


def aggregate_by_category(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("product_category", as_index=False)["revenue"]
        .sum()
        .sort_values("revenue", ascending=False)
        .reset_index(drop=True)
    )
