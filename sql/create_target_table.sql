CREATE TABLE IF NOT EXISTS public.orders_clean (
  order_id varchar PRIMARY KEY,
  user_id varchar,
  product_id varchar,
  quantity numeric,
  price numeric,
  order_date date,
  status varchar,
  total_price numeric
);
