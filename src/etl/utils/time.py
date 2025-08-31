import os
import pendulum as p


APP_TZ = os.getenv("APP_TZ", "Europe/Athens")

def now_utc_iso() -> str:
    return p.now("UTC").to_iso8601_string()


def default_lookback_iso(days: int) -> str:
    return p.now("UTC").subtract(days=days).to_iso8601_string()