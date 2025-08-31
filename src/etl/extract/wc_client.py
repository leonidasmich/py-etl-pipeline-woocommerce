from dotenv import load_dotenv
load_dotenv()

import os
from typing import Dict, Any, List
from woocommerce import API


class WooClient:
    def __init__(self):
        url = os.getenv("WC_BASE_URL", "").strip().rstrip("/") + "/"
        ck = os.getenv("WC_CONSUMER_KEY")
        cs = os.getenv("WC_CONSUMER_SECRET")

        if not url or not ck or not cs:
            raise RuntimeError("Woo credentials missing: set WC_BASE_URL, WC_CONSUMER_KEY, WC_CONSUMER_SECRET")

        # Using query_string_auth=True helps with hosts that block Basic Auth or add WAF rules (e.g., Cloudflare)
        self.wcapi = API(
            url=url,
            consumer_key=ck,
            consumer_secret=cs,
            version="wc/v3",
            timeout=60,
            wp_api=True,
            query_string_auth=True,
        )

    def get(self, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        resp = self.wcapi.get(path.lstrip("/"), params=params)
        # woocommerce lib returns a requests.Response-like object
        if resp.status_code >= 400:
            raise RuntimeError(f"Woo GET {path} failed {resp.status_code}: {resp.text}")
        return resp.json()

    def paged(self, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        page = 1
        per_page = int(params.get("per_page", 100))
        out: List[Dict[str, Any]] = []
        while True:
            q = {**params, "page": page, "per_page": per_page}
            data = self.get(path, q)
            if not data:
                break
            out.extend(data)
            if len(data) < per_page:
                break
            page += 1
        return out
