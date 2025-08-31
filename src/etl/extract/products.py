# src/etl/extract/products.py
from typing import Dict, List, Iterable, Set
from .wc_client import WooClient


def _chunks(seq: Iterable[int], size: int = 100):
    buf = []
    for x in seq:
        if x is None:
            continue
        buf.append(int(x))
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf


def _fetch_product_single(wc: WooClient, pid: int) -> dict | None:
    try:
        # Request full payload; some hosts hide nested fields with limited context
        p = wc.get(f"products/{pid}", params={"status": "any", "context": "edit"})
        if isinstance(p, list):
            p = p[0] if p else None
        return p or None
    except Exception:
        return None


def fetch_products_by_ids(product_ids: List[int]) -> Dict[int, dict]:
    """
    Return {product_id: product_json_with_categories}.
    Strategy:
      1) Try batching with ?include=... (fast) using context=edit.
      2) For any missing IDs OR products with empty categories, GET /products/{id} individually.
    """
    ids: List[int] = sorted({int(i) for i in product_ids if i is not None})
    if not ids:
        return {}

    wc = WooClient()
    out: Dict[int, dict] = {}

    # ---- 1) Batch attempt (no _fields; full payload; context=edit)
    for batch in _chunks(ids, size=100):
        try:
            data = wc.get(
                "products",
                params={
                    "include": ",".join(str(i) for i in batch),
                    "per_page": 100,
                    "status": "any",
                    "context": "edit",
                },
            )
        except Exception:
            data = []

        for p in data or []:
            pid = p.get("id")
            if pid is not None:
                out[int(pid)] = p

    # ---- 2) Fallback per-ID for anything missing or with empty categories
    fetched_ids: Set[int] = set(out.keys())
    need_fallback: List[int] = [i for i in ids if (i not in fetched_ids) or not ((out.get(i) or {}).get("categories") or [])]

    for pid in need_fallback:
        p = _fetch_product_single(wc, pid)
        if p:
            out[int(pid)] = p  # overwrite if categories were empty

    return out
