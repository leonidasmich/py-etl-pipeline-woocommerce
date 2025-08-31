import pandas as pd
from typing import Dict, Tuple


def enrich_items_with_categories(df_items: pd.DataFrame, products: Dict[int, dict]) -> pd.DataFrame:
    """
    Adds a 'category_snapshot' string to each item by looking up the product's categories.
    """
    if df_items.empty or not products:
        return df_items

    def cat_str(pid):
        try:
            p = products.get(int(pid)) if pid is not None else None
        except Exception:
            p = None
        cats = (p or {}).get("categories") or []
        names = [c.get("name") for c in cats if c.get("name")]
        return " | ".join(names) if names else None

    df = df_items.copy()
    df["category_snapshot"] = df["product_id"].apply(cat_str)
    return df


def apply_refunds(
    df_orders: pd.DataFrame,
    df_items: pd.DataFrame,
    refunds_map: Dict[int, dict],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Applies order-level and item-level refunds.
    - df_orders: adds 'refund_total' and 'net_after_refunds'
    - df_items: adds 'refunded_quantity' and 'refunded_total'
    """
    if df_orders.empty:
        return df_orders, df_items

    dfo = df_orders.copy()
    dfi = df_items.copy()

    # Order-level refunds
    def order_refund_total(oid):
        m = refunds_map.get(int(oid), {})
        try:
            return float(m.get("refund_total", 0.0))
        except Exception:
            return 0.0

    dfo["refund_total"] = dfo["order_id"].apply(order_refund_total)
    dfo["net_after_refunds"] = dfo["net_total"] - dfo["refund_total"]

    # Item-level refunds (by product_id + variation_id)
    if not dfi.empty:
        def ref_qty(row):
            m = refunds_map.get(int(row["order_id"])) or {}
            items = m.get("items", {})
            key = (int(row.get("product_id") or 0), int(row.get("variation_id") or 0))
            return int((items.get(key) or {}).get("qty", 0))

        def ref_total(row):
            m = refunds_map.get(int(row["order_id"])) or {}
            items = m.get("items", {})
            key = (int(row.get("product_id") or 0), int(row.get("variation_id") or 0))
            try:
                return float((items.get(key) or {}).get("total", 0.0))
            except Exception:
                return 0.0

        dfi["refunded_quantity"] = dfi.apply(ref_qty, axis=1)
        dfi["refunded_total"] = dfi.apply(ref_total, axis=1)

    return dfo, dfi
