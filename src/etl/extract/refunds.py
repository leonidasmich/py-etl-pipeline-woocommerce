# src/etl/extract/refunds.py
from typing import Dict, List, Tuple
from .wc_client import WooClient


def fetch_refunds_for_orders(order_ids: List[int]) -> Dict[int, dict]:
    """
    Returns a mapping:
      {
        order_id: {
          "refund_total": float,           # total refunded amount for the order
          "items": {                       # per (product_id, variation_id)
            (product_id, variation_id): {
               "qty": int,
               "total": float
            }, ...
          }
        }, ...
      }
    """
    wc = WooClient()
    result: Dict[int, dict] = {}

    for oid in order_ids or []:
        try:
            resp = wc.get(f"orders/{int(oid)}/refunds", params={"per_page": 100})
        except Exception:
            resp = []

        total_amt = 0.0
        items_map: Dict[Tuple[int, int], dict] = {}

        for r in resp or []:
            # Order-level refund amount (string in Woo, cast to float)
            try:
                total_amt += float(r.get("amount") or 0)
            except Exception:
                pass

            # Item-level refunds
            for li in (r.get("line_items") or []):
                pid = int(li.get("product_id") or 0)
                vid = int(li.get("variation_id") or 0)
                key = (pid, vid)
                entry = items_map.setdefault(key, {"qty": 0, "total": 0.0})

                try:
                    entry["qty"] += int(li.get("quantity") or 0)
                except Exception:
                    pass
                try:
                    entry["total"] += float(li.get("total") or 0)
                except Exception:
                    pass

        result[int(oid)] = {
            "refund_total": total_amt,
            "items": items_map,
        }

    return result
