import os
import duckdb

DB = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
con = duckdb.connect(DB)

def ensure_columns(table, columns_sql):
    # columns_sql: list of tuples (col_name, add_sql)
    existing = set(con.execute(f"PRAGMA table_info('{table}')").df()["name"].tolist())
    for col, add_sql in columns_sql:
        if col not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {add_sql}")
            print(f"Added {table}.{col}")

# fct_orders new columns
ensure_columns("fct_orders", [
    ("refund_total", "refund_total DOUBLE"),
    ("net_after_refunds", "net_after_refunds DOUBLE"),
])

# fct_order_items new columns
ensure_columns("fct_order_items", [
    ("category_snapshot", "category_snapshot VARCHAR"),
    ("refunded_quantity", "refunded_quantity INTEGER"),
    ("refunded_total", "refunded_total DOUBLE"),
])

print("Migration complete.")
