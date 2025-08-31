from dotenv import load_dotenv
load_dotenv()

import argparse
import os
import duckdb
import pendulum as p
import pandas as pd

from src.etl.extract.orders import fetch_orders_since
from src.etl.extract.products import fetch_products_by_ids
from src.etl.extract.refunds import fetch_refunds_for_orders
from src.etl.transform.normalize_orders import normalize_orders
from src.etl.transform.enrich import enrich_items_with_categories, apply_refunds
from src.etl.load.duckdb_client import DuckDBClient
from src.etl.utils.state import get_since_ts, set_since_ts
from src.etl.utils.logging import get_logger

log = get_logger(__name__)
DB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")


def _process_batch(raw_orders):
    """Normalize -> enrich -> refunds -> load. Returns (n_orders, n_items, max_order_dt_or_None)."""
    if not raw_orders:
        return 0, 0, None

    # Normalize
    df_orders, df_items = normalize_orders(raw_orders)
    log.info(f"Normalized: orders={len(df_orders)}, items={len(df_items)}")

    # Enrich categories (for this batch’s product_ids)
    product_ids = sorted({int(x) for x in df_items["product_id"].dropna().unique().tolist()}) if not df_items.empty else []
    products = fetch_products_by_ids(product_ids)
    df_items = enrich_items_with_categories(df_items, products)

    # Apply refunds (orders + items)
    order_ids = df_orders["order_id"].tolist()
    refunds_map = fetch_refunds_for_orders(order_ids)
    df_orders, df_items = apply_refunds(df_orders, df_items, refunds_map)

    # Load
    db = DuckDBClient()
    db.init_schema()
    db.load_orders(df_orders)
    db.load_order_items(df_items)

    max_dt = df_orders["order_date"].max() if not df_orders.empty else None
    return len(df_orders), len(df_items), max_dt


def _re_enrich_categories(force_all: bool = False) -> int:
    """Re-enrich category_snapshot for existing rows. Returns number of products attempted."""
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
        log.info("Re-enrich: nothing to do.")
        return 0

    pids = [int(x) for x in need["product_id"].dropna().tolist()]
    log.info(f"Re-enrich: fetching {len(pids)} products…")
    products = fetch_products_by_ids(pids)

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
    log.info("Re-enrich: done.")
    return len(pids)


def _backfill(start_iso: str, window_days: int = 30):
    """Backfill from start date to now in windows. Updates watermark as it goes."""
    start = p.parse(start_iso)
    end = p.now("UTC")
    cursor = start
    total_orders = 0
    log.info(f"Backfill from {start.to_iso8601_string()} to {end.to_iso8601_string()} in {window_days}-day windows")

    while cursor < end:
        window_end = min(cursor.add(days=window_days), end)
        # Woo supports 'after' param; we bound window by advancing watermark ourselves
        raw = fetch_orders_since(cursor.to_iso8601_string())
        n_orders, n_items, max_dt = _process_batch(raw)
        total_orders += n_orders
        # advance cursor conservatively
        if max_dt:
            cursor = p.parse(max_dt).add(minutes=1)
            set_since_ts(cursor.to_iso8601_string())
            log.info(f"Backfill window loaded: orders={n_orders}; watermark={cursor.to_iso8601_string()}")
        else:
            # no data; jump window
            cursor = window_end

    # Final re-enrich pass for any lingering uncategorized
    _re_enrich_categories(force_all=False)
    log.info(f"Backfill complete. Total orders loaded: {total_orders}")


def main():
    ap = argparse.ArgumentParser(description="WooCommerce ETL runner")
    ap.add_argument("--re-enrich", action="store_true", help="Re-enrich categories for existing items that are missing them")
    ap.add_argument("--force-enrich-all", action="store_true", help="Re-enrich categories for ALL items (overwrites existing)")
    ap.add_argument("--backfill-start", type=str, help="ISO date (YYYY-MM-DD) to backfill from")
    args = ap.parse_args()

    # Backfill mode
    if args.backfill_start:
        start_iso = p.parse(args.backfill_start).to_iso8601_string()
        _backfill(start_iso)
        return

    # Incremental ETL
    since_iso = get_since_ts()
    log.info(f"Starting ETL since={since_iso}")
    raw_orders = fetch_orders_since(since_iso)
    log.info(f"Fetched {len(raw_orders)} orders")

    if raw_orders:
        n_orders, n_items, max_dt = _process_batch(raw_orders)
        if max_dt:
            watermark = p.parse(max_dt).add(minutes=1).to_iso8601_string()
            set_since_ts(watermark)
            log.info(f"Done. New watermark={watermark}")
    else:
        log.info("No new orders.")

    # Re-enrich pass:
    #  - if user requested explicitly OR
    #  - if no new orders were fetched (keep categories fresh without extra commands)
    if args.force_enrich_all:
        _re_enrich_categories(force_all=True)
    elif args.re_enrich or not raw_orders:
        _re_enrich_categories(force_all=False)


if __name__ == "__main__":
    main()
