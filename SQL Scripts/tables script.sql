CREATE DATABASE IF NOT EXISTS DB_etl;
USE DB_etl;




CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(30),
    email VARCHAR(255),
    is_verified BOOLEAN,
    UNIQUE (email)
);

-- =========================
--  MERCHANTS (DIM)
-- =========================
CREATE TABLE merchants (
    merchant_id VARCHAR(50) PRIMARY KEY,
    business_name VARCHAR(255),
    contact_name VARCHAR(255),
    phone VARCHAR(30),
    email VARCHAR(255),
    category VARCHAR(100)
);

-- =========================
--  DRIVERS (DIM)
-- =========================
CREATE TABLE drivers (
    driver_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(30),
    vehicle_type VARCHAR(50),
    vehicle_plate VARCHAR(50),
    rating DECIMAL(3,2),
    total_deliveries INT
);

-- =========================
--  ADDRESSES (DIM)
-- =========================
CREATE TABLE addresses (
    address_id VARCHAR(50) PRIMARY KEY,
    floor VARCHAR(20),
    apartment VARCHAR(20),
    building VARCHAR(50),
    street VARCHAR(255),
    area VARCHAR(255),
    city VARCHAR(100),
    district VARCHAR(100),
    governorate VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    country_code VARCHAR(10),
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    landmark VARCHAR(255),
    special_instructions TEXT
);

-- =========================
--  TRACKING (DIM)
-- =========================
CREATE TABLE tracking (
    tracker_id VARCHAR(50) PRIMARY KEY,
    tracking_url TEXT,
    current_status VARCHAR(50),
    estimated_delivery_time DATETIME
);

-- =========================
--  PAYMENTS 
-- =========================
CREATE TABLE payments (
    payment_id VARCHAR(50) PRIMARY KEY,
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    currency VARCHAR(10),
    subtotal DECIMAL(12,2),
    delivery_fee DECIMAL(12,2),
    service_fee DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    total_amount DECIMAL(12,2),
    collected_amount DECIMAL(12,2),
    is_paid_back BOOLEAN,
    collected_at DATETIME,
    business_collection_date DATETIME,
    business_collection_status VARCHAR(50)
);

-- =========================
--  ORDERS (CORE FACT)
-- =========================
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    order_number VARCHAR(50),
    order_type VARCHAR(50),
    order_status VARCHAR(50),
    created_at DATETIME,
    updated_at DATETIME,
    scheduled_pickup_time DATETIME,
    actual_pickup_time DATETIME,
    scheduled_delivery_time DATETIME,
    actual_delivery_time DATETIME,

    customer_id VARCHAR(50),
    merchant_id VARCHAR(50),
    driver_id VARCHAR(50),
    pickup_address_id VARCHAR(50),
    dropoff_address_id VARCHAR(50),
    payment_id VARCHAR(50),
    tracker_id VARCHAR(50),

    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    CONSTRAINT fk_order_merchant FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id),
    CONSTRAINT fk_order_driver FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
    CONSTRAINT fk_order_pickup_address FOREIGN KEY (pickup_address_id) REFERENCES addresses(address_id),
    CONSTRAINT fk_order_dropoff_address FOREIGN KEY (dropoff_address_id) REFERENCES addresses(address_id),
    CONSTRAINT fk_order_payment FOREIGN KEY (payment_id) REFERENCES payments(payment_id),
    CONSTRAINT fk_order_tracking FOREIGN KEY (tracker_id) REFERENCES tracking(tracker_id)
);

-- =========================
--  ITEMS 
-- =========================
CREATE TABLE items (
    item_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    sku VARCHAR(100),
    name VARCHAR(255),
    description TEXT,
    category VARCHAR(100),
    quantity INT,
    unit_price DECIMAL(12,2),
    total_price DECIMAL(12,2),
    weight_kg DECIMAL(8,2),
    length_cm DECIMAL(8,2),
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),

    CONSTRAINT fk_item_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- =========================
--  ORDER ACTIONS 
-- =========================
CREATE TABLE order_actions (
    action_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    action_type VARCHAR(100),
    status VARCHAR(50),
    timestamp DATETIME,
    performed_by VARCHAR(50),
    performed_by_id VARCHAR(50),
    notes TEXT,
    latitude DECIMAL(10,7),
    longitude DECIMAL(10,7),
    driver_id VARCHAR(50),
    signature_url TEXT,
    photo_url TEXT,
    received_by VARCHAR(255),

    CONSTRAINT fk_action_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- =========================
-- ORDER NOTES 
-- =========================
CREATE TABLE order_notes (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_notes TEXT,
    merchant_notes TEXT,
    driver_notes TEXT,
    internal_notes TEXT,

    CONSTRAINT fk_notes_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- =========================
--  ORDER METADATA 
-- =========================
CREATE TABLE order_metadata (
    order_id VARCHAR(50) PRIMARY KEY,
    source_platform VARCHAR(50),
    app_version VARCHAR(50),
    device_type VARCHAR(50),
    promo_code VARCHAR(50),
    is_first_order BOOLEAN,
    customer_rating INT,
    customer_feedback TEXT,
    driver_rating INT,
    rated_at DATETIME,

    CONSTRAINT fk_metadata_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- =========================
-- PERFORMANCE INDEXES
-- =========================
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_items_order_id ON items(order_id);
CREATE INDEX idx_actions_order_id ON order_actions(order_id);
CREATE INDEX idx_actions_timestamp ON order_actions(timestamp);
