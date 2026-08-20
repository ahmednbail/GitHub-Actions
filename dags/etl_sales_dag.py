
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data" / "raw_sales.csv"
CLEAN_PATH = PROJECT_ROOT / "data" / "clean_sales.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "sales_by_category.csv"


def extract() -> None:
    """Read the raw CSV and pass it forward unchanged."""
    df = pd.read_csv(RAW_PATH)
    print(f"[extract] read {len(df)} rows from {RAW_PATH}")


def transform() -> None:
    """Clean nulls, drop negative revenue, and aggregate by category."""
    df = pd.read_csv(RAW_PATH)

    # Drop rows missing critical fields
    df = df.dropna(subset=["customer_id", "product_category", "revenue"])

    df = df[df["revenue"] >= 0]

    df.to_csv(CLEAN_PATH, index=False)
    print(f"[transform] cleaned data -> {len(df)} rows written to {CLEAN_PATH}")

    # Aggregate revenue per category
    agg = (
        df.groupby("product_category", as_index=False)["revenue"]
        .sum()
        .sort_values("revenue", ascending=False)
    )
    agg.to_csv(OUTPUT_PATH, index=False)
    print(f"[transform] aggregated by category -> {OUTPUT_PATH}")


def load() -> None:
    if not OUTPUT_PATH.exists():
        raise FileNotFoundError(f"Expected {OUTPUT_PATH} to exist after transform")
    df = pd.read_csv(OUTPUT_PATH)
    print(f"[load] would load {len(df)} aggregated rows to warehouse")


# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_sales",
    description="Daily ETL of sales data, aggregated by product category",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "sales", "demo"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> transform_task >> load_task
