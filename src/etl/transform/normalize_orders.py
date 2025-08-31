from typing import List, Dict, Tuple
import pandas as pd
import pendulum as p


def _f(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def normalize_orders(raw_orders: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Flatten Woo order JSON into:
      - df_orders (one row per order)
      - df_items  (one row per line item)
    Adds placeholders for refund enrichment and category snapshot.
    """
    orders_rows = []
    items_rows = []

    for o in raw_orders or []:
        order_id = o.get("id")
        created = o.get("date_created_gmt") or o.get("date_created")  # fallback just in case

        row = {
            "order_id": order_id,
            "order_date": p.parse(created).to_datetime_string() if created else None,
            "status": o.get("status"),
            "currency": o.get("currency"),
            "customer_id": o.get("customer_id"),
            "discount_total": _f(o.get("discount_total")),
            "discount_tax": _f(o.get("discount_tax")),
            "shipping_total": _f(o.get("shipping_total")),
            "shipping_tax": _f(o.get("shipping_tax")),
            "cart_tax": _f(o.get("cart_tax")),
            "total_tax": _f(o.get("total_tax")),
            "gross_total": _f(o.get("total")),
            # Baseline net (pre-refund); refunds applied later
            "net_total": _f(o.get("total")) - _f(o.get("total_tax")),
            # Refund enrichment placeholders
            "refund_total": 0.0,
            "net_after_refunds": None,
            # Light geo
            "billing_country": (o.get("billing") or {}).get("country"),
            "billing_city": (o.get("billing") or {}).get("city"),
        }
        orders_rows.append(row)

        for li in o.get("line_items", []) or []:
            items_rows.append(
                {
                    "order_id": order_id,
                    "product_id": li.get("product_id"),
                    "variation_id": li.get("variation_id"),
                    "sku": li.get("sku"),
                    "name": li.get("name"),
                    "quantity": int(li.get("quantity") or 0),
                    "price": _f(li.get("price")),
                    "total": _f(li.get("total")),
                    "subtotal": _f(li.get("subtotal")),
                    "tax_class": li.get("tax_class"),
                    # Enrichment placeholders
                    "category_snapshot": None,
                    "refunded_quantity": 0,
                    "refunded_total": 0.0,
                }
            )

    df_orders = pd.DataFrame(orders_rows)
    df_items = pd.DataFrame(items_rows)

    if not df_orders.empty and "order_date" in df_orders.columns:
        df_orders.sort_values("order_date", inplace=True)

    return df_orders, df_items
