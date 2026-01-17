/* =========================================================
   ORDERS TABLE INDEXES
   Purpose: Speed up filtering and joins on the core fact table
   ========================================================= */

/* Filter orders by status (e.g. DELIVERED, CANCELLED) */
CREATE INDEX idx_orders_status
ON orders(order_status);

/* Filter and sort orders by creation date (dashboards, ETL windows) */
CREATE INDEX idx_orders_created_at
ON orders(created_at);

/* Join orders → customers */
CREATE INDEX idx_orders_customer_id
ON orders(customer_id);

/* Join orders → merchants */
CREATE INDEX idx_orders_merchant_id
ON orders(merchant_id);

/* Join orders → drivers */
CREATE INDEX idx_orders_driver_id
ON orders(driver_id);

/* Join orders → payments */
CREATE INDEX idx_orders_payment_id
ON orders(payment_id);



/* =========================================================
   ITEMS TABLE INDEXES
   Purpose: Speed up order → items lookups and SKU searches
   ========================================================= */

/* Join items → orders (1 order → many items) */
CREATE INDEX idx_items_order_id
ON items(order_id);

/* Search or aggregate items by SKU */
CREATE INDEX idx_items_sku
ON items(sku);



/* =========================================================
   ORDER ACTIONS (EVENT LOG) INDEXES
   Purpose: Fast retrieval of order timelines
   ========================================================= */

/* 
   Retrieve full event history for an order
   Optimized for:
   WHERE order_id = ?
   ORDER BY timestamp
*/
CREATE INDEX idx_actions_order_time
ON order_actions(order_id, timestamp);



/* =========================================================
   PAYMENTS TABLE INDEXES
   Purpose: Filter payments by lifecycle state
   ========================================================= */

/* Filter PAID / UNPAID payments */
CREATE INDEX idx_payments_status
ON payments(payment_status);



/* =========================================================
   ADDRESSES TABLE INDEXES
   Purpose: Geographic filtering & reporting
   ========================================================= */

/* Filter or group addresses by city */
CREATE INDEX idx_addresses_city
ON addresses(city);

/* Filter addresses by area (zones, neighborhoods) */
CREATE INDEX idx_addresses_area
ON addresses(area);






/* =========================================================
   Query 1: Delivered orders with customer & merchant info
   Purpose: Order tracking dashboards & reporting
   ========================================================= */

SELECT
    o.order_id,
    o.order_status,
    c.first_name,
    m.business_name
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id
JOIN merchants m
    ON o.merchant_id = m.merchant_id
WHERE o.order_status = 'DELIVERED';



/* =========================================================
   Query 2: Order activity timeline
   Purpose: Customer support, tracking, auditing
   ========================================================= */

SELECT
    action_type,
    status,
    timestamp
FROM order_actions
WHERE order_id = 'ORD123'
ORDER BY timestamp;


/* =========================================================
   Query 3: Paid orders and their total amounts
   Purpose: Finance & reconciliation reports
   ========================================================= */

SELECT
    o.order_id,
    p.total_amount
FROM orders o
JOIN payments p
    ON o.payment_id = p.payment_id
WHERE p.payment_status = 'PAID';


