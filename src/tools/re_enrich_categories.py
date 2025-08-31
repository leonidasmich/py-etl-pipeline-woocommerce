from dotenv import load_dotenv
load_dotenv()

import os
import duckdb
import pandas as pd
from src.etl.extract.products import fetch_products_by_ids

DB = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")

def main():
    con = duckdb.connect(DB)

    # 1) Find product_ids that need enrichment (NULL or empty category_snapshot)
    need = con.execute("""
        SELECT DISTINCT product_id
        FROM fct_order_items
        WHERE product_id IS NOT NULL
          AND (category_snapshot IS NULL OR TRIM(category_snapshot) = '')
    """).df()

    if need.empty:
        print("Nothing to enrich. All items already have categories.")
        return

    pids = [int(x) for x in need["product_id"].dropna().tolist()]
    print(f"Enriching {len(pids)} products with categories...")

    # 2) Fetch products (robust fetch that tries batch + single)
    products = fetch_products_by_ids(pids)

    # 3) Build mapping product_id -> category string
    def cat_str(pid):
        p = products.get(int(pid))
        cats = (p or {}).get("categories") or []
        names = [c.get("name") for c in cats if c.get("name")]
        return " | ".join(names) if names else None

    map_df = pd.DataFrame({
        "product_id": pids,
        "category_snapshot": [cat_str(pid) for pid in pids]
    })

    # 4) Load mapping into DuckDB and UPDATE via join
    con.register("map_df", map_df)
    con.execute("""
        UPDATE fct_order_items AS i
        SET category_snapshot = m.category_snapshot
        FROM map_df AS m
        WHERE i.product_id = m.product_id
          AND (i.category_snapshot IS NULL OR TRIM(i.category_snapshot) = '')
    """)

    # Optional: show how many got updated
    updated = con.execute("""
        SELECT COUNT(*) AS n
        FROM fct_order_items
        WHERE category_snapshot IS NOT NULL AND TRIM(category_snapshot) <> ''
    """).df().iloc[0]["n"]

    print(f"Done. Items with non-empty category_snapshot: {int(updated)}")

if __name__ == "__main__":
    main()
