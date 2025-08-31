from dotenv import load_dotenv
load_dotenv()

import pendulum as p
import duckdb
import pandas as pd
from typing import Tuple

from prefect import flow, task, get_run_logger

from src.etl.utils.state import get_since_ts, set_since_ts
from src.etl.extract.orders import fetch_orders_since
from src.etl.extract.products import fetch_products_by_ids
from src.etl.extract.refunds import fetch_refunds_for_orders
from src.etl.transform.normalize_orders import normalize_orders
from src.etl.transform.enrich import enrich_items_with_categories, apply_refunds
from src.etl.load.duckdb_client import DuckDBClient

import os
DB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")


# ---------- Core Tasklets ----------

@task
def t_normalize(raw) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return normalize_orders(raw)

@task
def t_enrich_items(df_items: pd.DataFrame, products: dict) -> pd.DataFrame:
    return enrich_items_with_categories(df_items, products)

@task
def t_apply_refunds(df_orders: pd.DataFrame, df_items: pd.DataFrame, refunds_map: dict):
    return apply_refunds(df_orders, df_items, refunds_map)

@task
def t_load(df_orders: pd.DataFrame, df_items: pd.DataFrame):
    db = DuckDBClient()
    db.init_schema()
    db.load_orders(df_orders)
    db.load_order_items(df_items)

@task(retries=2, retry_delay_seconds=30)
def t_fetch_orders(since_iso: str):
    return fetch_orders_since(since_iso)

@task
def t_fetch_products(product_ids):
    return fetch_products_by_ids(product_ids)

@task
def t_fetch_refunds(order_ids):
    return fetch_refunds_for_orders(order_ids)


# ---------- Helpers wrapped as tasks ----------

@task
def t_re_enrich_categories(force_all: bool = False) -> int:
    """Re-enrich category_snapshot in-place for existing rows. Returns number of products attempted."""
    con = duckdb.connect(DB_PATH)
    if force_all:
        need = con.execute("""
            SELECT DISTINCT product_id
            FROM fct_order_items
            WHERE product_id IS NOT NULL
        """).df()
    else:
        need = con.execute("""
            SELECT DISTINCT product_id
            FROM fct_order_items
            WHERE product_id IS NOT NULL
              AND (category_snapshot IS NULL OR TRIM(category_snapshot) = '')
        """).df()

    if need.empty:
        return 0

    pids = [int(x) for x in need["product_id"].dropna().tolist()]
    products = fetch_products_by_ids(pids)  # direct call OK here (pure function)

    def cat_str(pid):
        p = products.get(int(pid))
        cats = (p or {}).get("categories") or []
        names = [c.get("name") for c in cats if c.get("name")]
        return " | ".join(names) if names else None

    map_df = pd.DataFrame({
        "product_id": pids,
        "category_snapshot": [cat_str(pid) for pid in pids]
    })
    con.register("map_df", map_df)
    con.execute("""
        UPDATE fct_order_items AS i
        SET category_snapshot = m.category_snapshot
        FROM map_df AS m
        WHERE i.product_id = m.product_id
          AND (? OR i.category_snapshot IS NULL OR TRIM(i.category_snapshot) = '')
    """, [force_all])
    con.close()
    return len(pids)


@task
def t_advance_watermark(df_orders: pd.DataFrame) -> str | None:
    if df_orders is None or df_orders.empty:
        return None
    max_dt = df_orders["order_date"].max()
    if not max_dt:
        return None
    watermark = p.parse(max_dt).add(minutes=1).to_iso8601_string()
    set_since_ts(watermark)
    return watermark


# ---------- Batch processor (as a task) ----------

@task
def t_process_batch(raw_orders) -> Tuple[int, int, str | None]:
    """
    Normalize -> enrich -> refunds -> load. Return (n_orders, n_items, new_watermark_or_None).
    """
    logger = get_run_logger()
    if not raw_orders:
        return 0, 0, None

    # Normalize
    df_orders, df_items = normalize_orders(raw_orders)  # local: avoid task overhead
    logger.info(f"Normalized: orders={len(df_orders)}, items={len(df_items)}")

    # Enrich categories (for this batch’s product_ids)
    product_ids = sorted({int(x) for x in df_items["product_id"].dropna().unique().tolist()}) if not df_items.empty else []
    products = fetch_products_by_ids(product_ids)
    df_items = enrich_items_with_categories(df_items, products)

    # Apply refunds
    order_ids = df_orders["order_id"].tolist()
    refunds_map = fetch_refunds_for_orders(order_ids)
    df_orders, df_items = apply_refunds(df_orders, df_items, refunds_map)

    # Load
    db = DuckDBClient()
    db.init_schema()
    db.load_orders(df_orders)
    db.load_order_items(df_items)

    # Watermark
    max_dt = df_orders["order_date"].max() if not df_orders.empty else None
    watermark = p.parse(max_dt).add(minutes=1).to_iso8601_string() if max_dt else None
    if watermark:
        set_since_ts(watermark)
    return len(df_orders), len(df_items), watermark


# ---------- Flows ----------

@flow(name="woocommerce-etl-flow")
def run_flow(
    re_enrich: bool = False,
    force_enrich_all: bool = False,
    backfill_start: str | None = None,
    window_days: int = 30,
):
    """
    Unified Prefect flow:
      - If backfill_start is provided: backfill in windows, then re-enrich missing categories.
      - Else: run incremental ETL; if no new orders, optionally re-enrich missing categories.
      - `force_enrich_all` overwrites categories for all items.
    """
    logger = get_run_logger()

    # Backfill mode
    if backfill_start:
        start = p.parse(backfill_start)
        end = p.now("UTC")
        cursor = start
        total_orders = 0
        logger.info(f"Backfill from {start.to_iso8601_string()} to {end.to_iso8601_string()} (window={window_days}d)")
        while cursor < end:
            # We use 'after=cursor' and let watermark advance inside the processor
            raw = t_fetch_orders(cursor.to_iso8601_string())
            n_orders, n_items, wm = t_process_batch(raw)
            total_orders += n_orders
            if wm:
                cursor = p.parse(wm)  # already +1 min inside
                logger.info(f"Loaded {n_orders} orders; watermark={wm}")
            else:
                cursor = min(cursor.add(days=window_days), end)
        # final re-enrich pass for missing
        if force_enrich_all:
            n = t_re_enrich_categories(force_all=True)
            logger.info(f"Re-enriched ALL categories for {n} products.")
        else:
            n = t_re_enrich_categories(force_all=False)
            logger.info(f"Re-enriched MISSING categories for {n} products.")
        logger.info(f"Backfill complete. Total orders loaded: {total_orders}")
        return

    # Incremental mode
    since = get_since_ts()
    logger.info(f"Incremental run since={since}")
    raw = t_fetch_orders(since)
    n_orders, n_items, wm = t_process_batch(raw)

    if n_orders == 0:
        logger.info("No new orders.")
        # If no new orders, auto re-enrich (or force-all if requested)
        if force_enrich_all:
            n = t_re_enrich_categories(force_all=True)
            logger.info(f"Re-enriched ALL categories for {n} products.")
        elif re_enrich or True:  # default: re-enrich missing when nothing new
            n = t_re_enrich_categories(force_all=False)
            logger.info(f"Re-enriched MISSING categories for {n} products.")
    else:
        logger.info(f"Loaded {n_orders} orders; watermark={wm}")


if __name__ == "__main__":
    # Local examples:
    # run_flow()  # incremental + auto re-enrich when nothing new
    # run_flow(re_enrich=True)  # incremental + force re-enrich missing
    # run_flow(force_enrich_all=True)  # overwrite categories for all items
    # run_flow(backfill_start="2022-01-01", window_days=30)  # backfill mode
    run_flow()
