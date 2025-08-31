import json
import os
from .time import default_lookback_iso


STATE_PATH = "./data/state.json"


def get_since_ts() -> str:
    os.makedirs("./data", exist_ok=True)
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f).get("since_iso")
    
    days = int(os.getenv("DEFAULT_LOOKBACK_DAYS", "30"))
    return default_lookback_iso(days)


def set_since_ts(iso_ts: str) -> None:
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"since_iso": iso_ts}, f)