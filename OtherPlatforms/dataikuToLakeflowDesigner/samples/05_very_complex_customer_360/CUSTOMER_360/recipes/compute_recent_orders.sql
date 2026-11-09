-- SQL recipe (PostgreSQL). Project variables are expanded by DSS before execution.
SELECT o.customer_id,
       COUNT(DISTINCT o.order_id)                                                              AS orders_6m,
       ROUND(SUM(l.quantity * COALESCE(l.unit_price, p.list_price) * (1 - l.discount_pct))::numeric, 2) AS revenue_6m,
       MAX(o.order_date)                                                                       AS last_order_date
FROM "erp"."orders" o
JOIN "erp"."order_lines" l ON l.order_id = o.order_id
LEFT JOIN "erp"."products" p ON p.product_id = l.product_id
WHERE o.status = 'COMPLETED'
  AND o.order_date >= DATE '${min_order_date}'
GROUP BY o.customer_id
