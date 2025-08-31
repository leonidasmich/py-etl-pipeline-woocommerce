from .wc_client import WooClient
from typing import List, Dict

def fetch_orders_since(since_iso: str, status: str | None = None) -> List[Dict]:
    """
    Fetch orders created after given ISO timestamp.
    NOTE: We intentionally do NOT use _fields, because WooCommerce does not reliably
    project nested fields (line_items.product_id, etc.). We need full line_items.
    """
    wc = WooClient()
    params = {
        "after": since_iso,
        "orderby": "date",
        "order": "asc",
        "per_page": 100,
    }
    if status:
        params["status"] = status
    return wc.paged("orders", params)