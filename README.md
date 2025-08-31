# WooCommerce → Warehouse ETL (Python)

A production-ready ETL that ingests **WooCommerce** orders, **enriches** items with product categories, **applies refunds/partials**, and loads into a **DuckDB** warehouse. Ships with a **Streamlit** dashboard and optional **email notifications** + **Prefect** orchestration.

## ✨ Features

* **Extract**: WooCommerce orders (REST via `woocommerce` lib), products, refunds.
* **Transform**: Normalized orders/items, derived net revenue, refund-aware metrics.
* **Enrich**: Item-level `category_snapshot` from products.
* **Load**: DuckDB tables: `fct_orders`, `fct_order_items`.
* **Incremental**: Watermark (`data/state.json`).
* **Orchestrate**: Prefect flow (local run or container).
* **Notify**: Email via SMTP on success/failure (optional).
* **Visualize**: Streamlit dashboard (KPIs, timeseries, top products, category mix, geo).

## 🛠️ Tech Stack

Python, pandas, DuckDB, Prefect, Streamlit, `woocommerce` API client, `python-dotenv`.

## 🚀 Quickstart (Local)

```bash
python -m venv .venv && source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
python -m src.run      # or: python -m src.etl.orchestration.flow
streamlit run src/dashboard/app.py
```

## 🔐 Environment Variables

Create a `.env` with:

```
WC_BASE_URL=https://yourstore.tld
WC_CONSUMER_KEY=ck_xxx
WC_CONSUMER_SECRET=cs_xxx

# Optional
APP_TZ=Europe/Athens
DUCKDB_PATH=./data/warehouse.duckdb
DEFAULT_LOOKBACK_DAYS=30

# Email notifications (optional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=youremail@gmail.com
SMTP_PASS=your_app_password
NOTIFY_TO=recipient@example.com
```

> For Gmail, use an **App Password** (2FA required).

## 🧱 Schema (core)

* `fct_orders(order_id, order_date, status, gross_total, net_total, refund_total, net_after_refunds, …)`
* `fct_order_items(order_id, product_id, name, quantity, total, category_snapshot, refunded_quantity, refunded_total, …)`

## ✅ Testing Email Notifications

```bash
python -m src.tools.test_notify
```

## 📝 License

MIT — do whatever, attribution appreciated.
