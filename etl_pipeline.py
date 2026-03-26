"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    # TODO: Implement extraction
    return {
        "customers": pd.read_sql("SELECT * FROM customers", engine),
        "products": pd.read_sql("SELECT * FROM products", engine),
        "orders": pd.read_sql("SELECT * FROM orders", engine),
        "order_items": pd.read_sql("SELECT * FROM order_items", engine),
    }


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    # TODO: Implement transformation
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    df = orders.merge(order_items, on="order_id")
    df = df.merge(products, on="product_id")
    df = df.merge(customers, on="customer_id")

    df["line_total"] = df["quantity"] * df["unit_price"]

    df = df[df["status"] != "cancelled"]
    df = df[df["quantity"] <= 100]

    agg = df.groupby(["customer_id", "customer_name", "city"]).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum"),
    ).reset_index()

    agg["avg_order_value"] = agg["total_revenue"] / agg["total_orders"]

    top_cat = (
        df.groupby(["customer_id", "category"])["line_total"]
        .sum()
        .reset_index()
        .sort_values(["customer_id", "line_total"], ascending=[True, False])
        .drop_duplicates("customer_id")
        .rename(columns={"category": "top_category"})
    )

    final_df = agg.merge(top_cat[["customer_id", "top_category"]], on="customer_id")

    return final_df


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    # TODO: Implement validation
    checks = {
        "no_null_customer_id": df["customer_id"].notnull().all(),
        "no_null_customer_name": df["customer_name"].notnull().all(),
        "positive_revenue": (df["total_revenue"] > 0).all(),
        "no_duplicate_ids": df["customer_id"].is_unique,
        "positive_orders": (df["total_orders"] > 0).all(),
    }

    if not all(checks.values()):
        raise ValueError(f"Validation failed: {checks}")

    return checks


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    # TODO: Implement loading
    df.to_sql("customer_summary", engine, if_exists="replace", index=False)

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    # TODO: Implement main orchestration
    # 1. Create engine from DATABASE_URL env var (or default)
    # 2. Extract
    # 3. Transform
    # 4. Validate
    # 5. Load to customer_summary table and output/customer_analytics.csv
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5434/amman_market"
    )

    engine = create_engine(db_url)

    data = extract(engine)
    transformed = transform(data)
    validate(transformed)

    load(
        transformed,
        engine,
        "output/customer_analytics.csv"
    )


if __name__ == "__main__":
    main()