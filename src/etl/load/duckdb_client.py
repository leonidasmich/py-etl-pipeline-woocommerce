# src/etl/load/duckdb_client.py
import os
from pathlib import Path
import duckdb
import pandas as pd
from ..utils.logging import get_logger

log = get_logger(__name__)

DB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
Path(os.path.dirname(DB_PATH) or ".").mkdir(parents=True, exist_ok=True)

# Column order we want in tables
FCT_ORDERS_COLS = [
    "order_id", "order_date", "status", "currency", "customer_id",
    "discount_total", "discount_tax", "shipping_total", "shipping_tax",
    "cart_tax", "total_tax", "gross_total", "net_total",
    "refund_total", "net_after_refunds",
    "billing_country", "billing_city",
]

FCT_ITEMS_COLS = [
    "order_id", "product_id", "variation_id", "sku", "name", "quantity",
    "price", "total", "subtotal", "tax_class",
    "category_snapshot", "refunded_quantity", "refunded_total",
]


class DuckDBClient:
    def __init__(self):
        self.con = duckdb.connect(DB_PATH)
        self.con.execute("PRAGMA threads=4")

    def init_schema(self):
        ddl_path = Path(__file__).with_name("ddl.sql")
        with open(ddl_path, "r", encoding="utf-8") as f:
            self.con.execute(f.read())
        log.info("Schema ensured.")

    def _align_cols(self, df: pd.DataFrame, cols: list) -> pd.DataFrame:
        df = df.copy()
        for c in cols:
            if c not in df.columns:
                df[c] = None
        # keep only desired columns in correct order
        return df[cols]

    def load_orders(self, df_orders: pd.DataFrame):
        if df_orders.empty:
            return
        df = self._align_cols(df_orders, FCT_ORDERS_COLS)

        ids = tuple(df["order_id"].unique().tolist())
        # Delete-then-insert to emulate upsert
        self.con.execute("DELETE FROM fct_orders WHERE order_id IN (SELECT * FROM UNNEST(?))", [ids])
        # DuckDB registers the pandas DF name as a view automatically
        self.con.execute("INSERT INTO fct_orders SELECT * FROM df")
        log.info(f"Loaded {len(df)} rows into fct_orders")

    def load_order_items(self, df_items: pd.DataFrame):
        if df_items.empty:
            return
        df = self._align_cols(df_items, FCT_ITEMS_COLS)

        ids = tuple(df["order_id"].unique().tolist())
        self.con.execute("DELETE FROM fct_order_items WHERE order_id IN (SELECT * FROM UNNEST(?))", [ids])
        self.con.execute("INSERT INTO fct_order_items SELECT * FROM df")
        log.info(f"Loaded {len(df)} rows into fct_order_items")