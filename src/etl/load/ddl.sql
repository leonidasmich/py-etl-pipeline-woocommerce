CREATE TABLE IF NOT EXISTS stg_orders_raw (
  order_id BIGINT,
  json JSON,
  extracted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fct_orders (
  order_id BIGINT PRIMARY KEY,
  order_date TIMESTAMP,
  status VARCHAR,
  currency VARCHAR,
  customer_id BIGINT,
  discount_total DOUBLE,
  discount_tax DOUBLE,
  shipping_total DOUBLE,
  shipping_tax DOUBLE,
  cart_tax DOUBLE,
  total_tax DOUBLE,
  gross_total DOUBLE,
  net_total DOUBLE,
  refund_total DOUBLE,
  net_after_refunds DOUBLE,
  billing_country VARCHAR,
  billing_city VARCHAR
);

CREATE TABLE IF NOT EXISTS fct_order_items (
  order_id BIGINT,
  product_id BIGINT,
  variation_id BIGINT,
  sku VARCHAR,
  name VARCHAR,
  quantity INTEGER,
  price DOUBLE,
  total DOUBLE,
  subtotal DOUBLE,
  tax_class VARCHAR,
  category_snapshot VARCHAR,
  refunded_quantity INTEGER,
  refunded_total DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_fct_order_items_order ON fct_order_items(order_id);
