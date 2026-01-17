
"""
load.py
Loads transformed data into MySQL using transactions.
Handles FK insert order and prevents duplicates using natural keys.
"""

import os
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Any, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Create MySQL connection using environment variables.
    
    Required environment variables:
        DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 3306)),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            autocommit=False
        )
        
        if connection.is_connected():
            logger.info(f"Connected to MySQL database: {os.getenv('DB_NAME')}")
            return connection
            
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise

#upsert logic for all tables by injecting sql queries
def upsert_customer(cursor, customer: Optional[Dict[str, Any]]) -> None:
    """
    Insert or update customer using phone as natural key.
    Fallback to email if phone is null.
    """
    if not customer or not customer.get('customer_id'):
        return
    
    query = """
        INSERT INTO customers (
            customer_id, first_name, last_name, phone, email, is_verified
        ) VALUES (
            %(customer_id)s, %(first_name)s, %(last_name)s, %(phone)s, %(email)s, %(is_verified)s
        )
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            email = VALUES(email),
            is_verified = VALUES(is_verified)
    """
    
    cursor.execute(query, customer)
    logger.debug(f"Upserted customer: {customer['customer_id']}")


def upsert_merchant(cursor, merchant: Optional[Dict[str, Any]]) -> None:
    """
    Insert or update merchant using merchant_id as natural key.
    """
    if not merchant or not merchant.get('merchant_id'):
        return
    
    query = """
        INSERT INTO merchants (
            merchant_id, business_name, contact_name, phone, email, category
        ) VALUES (
            %(merchant_id)s, %(business_name)s, %(contact_name)s, %(phone)s, %(email)s, %(category)s
        )
        ON DUPLICATE KEY UPDATE
            business_name = VALUES(business_name),
            contact_name = VALUES(contact_name),
            phone = VALUES(phone),
            email = VALUES(email),
            category = VALUES(category)
    """
    
    cursor.execute(query, merchant)
    logger.debug(f"Upserted merchant: {merchant['merchant_id']}")


def upsert_driver(cursor, driver: Optional[Dict[str, Any]]) -> None:
    """
    Insert or update driver using driver_id as natural key.
    """
    if not driver or not driver.get('driver_id'):
        return
    
    query = """
        INSERT INTO drivers (
            driver_id, first_name, last_name, phone, vehicle_type, 
            vehicle_plate, rating, total_deliveries
        ) VALUES (
            %(driver_id)s, %(first_name)s, %(last_name)s, %(phone)s, %(vehicle_type)s,
            %(vehicle_plate)s, %(rating)s, %(total_deliveries)s
        )
        ON DUPLICATE KEY UPDATE
            first_name = VALUES(first_name),
            last_name = VALUES(last_name),
            phone = VALUES(phone),
            vehicle_type = VALUES(vehicle_type),
            vehicle_plate = VALUES(vehicle_plate),
            rating = VALUES(rating),
            total_deliveries = VALUES(total_deliveries)
    """
    
    cursor.execute(query, driver)
    logger.debug(f"Upserted driver: {driver['driver_id']}")


def upsert_address(cursor, address: Optional[Dict[str, Any]]) -> None:
    """
    Insert or update address using generated hash as natural key.
    """
    if not address or not address.get('address_id'):
        return
    
    query = """
        INSERT INTO addresses (
            address_id, floor, apartment, building, street, area, city, district,
            governorate, postal_code, country, country_code, latitude, longitude,
            landmark, special_instructions
        ) VALUES (
            %(address_id)s, %(floor)s, %(apartment)s, %(building)s, %(street)s, %(area)s,
            %(city)s, %(district)s, %(governorate)s, %(postal_code)s, %(country)s,
            %(country_code)s, %(latitude)s, %(longitude)s, %(landmark)s, %(special_instructions)s
        )
        ON DUPLICATE KEY UPDATE
            floor = VALUES(floor),
            apartment = VALUES(apartment),
            building = VALUES(building),
            landmark = VALUES(landmark),
            special_instructions = VALUES(special_instructions)
    """
    
    cursor.execute(query, address)
    logger.debug(f"Upserted address: {address['address_id']}")


def upsert_payment(cursor, payment: Optional[Dict[str, Any]]) -> None:
    """Insert or update payment."""
    if not payment or not payment.get('payment_id'):
        return
    
    query = """
        INSERT INTO payments (
            payment_id, payment_method, payment_status, currency, subtotal,
            delivery_fee, service_fee, discount_amount, total_amount, collected_amount,
            is_paid_back, collected_at, business_collection_date, business_collection_status
        ) VALUES (
            %(payment_id)s, %(payment_method)s, %(payment_status)s,
            %(currency)s, %(subtotal)s, %(delivery_fee)s, %(service_fee)s,
            %(discount_amount)s, %(total_amount)s, %(collected_amount)s,
            %(is_paid_back)s, %(collected_at)s, %(business_collection_date)s,
            %(business_collection_status)s
        )
        ON DUPLICATE KEY UPDATE
            payment_method = VALUES(payment_method),
            payment_status = VALUES(payment_status),
            currency = VALUES(currency),
            subtotal = VALUES(subtotal),
            delivery_fee = VALUES(delivery_fee),
            service_fee = VALUES(service_fee),
            discount_amount = VALUES(discount_amount),
            total_amount = VALUES(total_amount),
            collected_amount = VALUES(collected_amount),
            is_paid_back = VALUES(is_paid_back),
            collected_at = VALUES(collected_at),
            business_collection_date = VALUES(business_collection_date),
            business_collection_status = VALUES(business_collection_status)
    """
    
    cursor.execute(query, payment)
    logger.debug(f"Upserted payment: {payment['payment_id']}")


def upsert_tracking(cursor, tracking: Optional[Dict[str, Any]]) -> None:
    """Insert or update tracking."""
    if not tracking or not tracking.get('tracker_id'):
        return
    
    query = """
        INSERT INTO tracking (
            tracker_id, tracking_url, current_status, estimated_delivery_time
        ) VALUES (
            %(tracker_id)s, %(tracking_url)s, %(current_status)s,
            %(estimated_delivery_time)s
        )
        ON DUPLICATE KEY UPDATE
            tracking_url = VALUES(tracking_url),
            current_status = VALUES(current_status),
            estimated_delivery_time = VALUES(estimated_delivery_time)
    """
    
    cursor.execute(query, tracking)
    logger.debug(f"Upserted tracking: {tracking['tracker_id']}")


def upsert_order(cursor, order: Dict[str, Any]) -> None:
    """
    Insert or update order using order_id as natural key.
    """
    query = """
        INSERT INTO orders (
            order_id, order_number, order_type, order_status, created_at, updated_at,
            scheduled_pickup_time, actual_pickup_time, scheduled_delivery_time, actual_delivery_time,
            customer_id, merchant_id, driver_id, pickup_address_id, dropoff_address_id,
            payment_id, tracker_id
        ) VALUES (
            %(order_id)s, %(order_number)s, %(order_type)s, %(order_status)s, %(created_at)s,
            %(updated_at)s, %(scheduled_pickup_time)s, %(actual_pickup_time)s,
            %(scheduled_delivery_time)s, %(actual_delivery_time)s, %(customer_id)s,
            %(merchant_id)s, %(driver_id)s, %(pickup_address_id)s, %(dropoff_address_id)s,
            %(payment_id)s, %(tracker_id)s
        )
        ON DUPLICATE KEY UPDATE
            order_number = VALUES(order_number),
            order_type = VALUES(order_type),
            order_status = VALUES(order_status),
            updated_at = VALUES(updated_at),
            scheduled_pickup_time = VALUES(scheduled_pickup_time),
            actual_pickup_time = VALUES(actual_pickup_time),
            scheduled_delivery_time = VALUES(scheduled_delivery_time),
            actual_delivery_time = VALUES(actual_delivery_time),
            customer_id = VALUES(customer_id),
            merchant_id = VALUES(merchant_id),
            driver_id = VALUES(driver_id),
            pickup_address_id = VALUES(pickup_address_id),
            dropoff_address_id = VALUES(dropoff_address_id),
            payment_id = VALUES(payment_id),
            tracker_id = VALUES(tracker_id)
    """
    
    cursor.execute(query, order)
    logger.debug(f"Upserted order: {order['order_id']}")


def upsert_items(cursor, items: List[Dict[str, Any]]) -> None:
    """Insert or update order items."""
    if not items:
        return
    
    query = """
        INSERT INTO items (
            item_id, order_id, sku, name, description, category, quantity,
            unit_price, total_price, weight_kg, length_cm, width_cm, height_cm
        ) VALUES (
            %(item_id)s, %(order_id)s, %(sku)s, %(name)s, %(description)s, %(category)s,
            %(quantity)s, %(unit_price)s, %(total_price)s, %(weight_kg)s, %(length_cm)s,
            %(width_cm)s, %(height_cm)s
        )
        ON DUPLICATE KEY UPDATE
            sku = VALUES(sku),
            name = VALUES(name),
            description = VALUES(description),
            category = VALUES(category),
            quantity = VALUES(quantity),
            unit_price = VALUES(unit_price),
            total_price = VALUES(total_price),
            weight_kg = VALUES(weight_kg),
            length_cm = VALUES(length_cm),
            width_cm = VALUES(width_cm),
            height_cm = VALUES(height_cm)
    """
    
    cursor.executemany(query, items)
    logger.debug(f"Upserted {len(items)} items")


def upsert_order_actions(cursor, actions: List[Dict[str, Any]]) -> None:
    """Insert or update order actions (audit log)."""
    if not actions:
        return
    
    query = """
        INSERT INTO order_actions (
            action_id, order_id, action_type, status, timestamp, performed_by,
            performed_by_id, notes, latitude, longitude, driver_id, signature_url,
            photo_url, received_by
        ) VALUES (
            %(action_id)s, %(order_id)s, %(action_type)s, %(status)s, %(timestamp)s,
            %(performed_by)s, %(performed_by_id)s, %(notes)s, %(latitude)s,
            %(longitude)s, %(driver_id)s, %(signature_url)s, %(photo_url)s,
            %(received_by)s
        )
        ON DUPLICATE KEY UPDATE
            action_type = VALUES(action_type),
            status = VALUES(status),
            timestamp = VALUES(timestamp),
            performed_by = VALUES(performed_by),
            performed_by_id = VALUES(performed_by_id),
            notes = VALUES(notes),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            driver_id = VALUES(driver_id),
            signature_url = VALUES(signature_url),
            photo_url = VALUES(photo_url),
            received_by = VALUES(received_by)
    """
    
    cursor.executemany(query, actions)
    logger.debug(f"Upserted {len(actions)} order actions")


def upsert_order_notes(cursor, notes: Optional[Dict[str, Any]]) -> None:
    """Insert or update order notes."""
    if not notes or not notes.get('order_id'):
        return
    
    query = """
        INSERT INTO order_notes (
            order_id, customer_notes, merchant_notes, driver_notes, internal_notes
        ) VALUES (
            %(order_id)s, %(customer_notes)s, %(merchant_notes)s, %(driver_notes)s,
            %(internal_notes)s
        )
        ON DUPLICATE KEY UPDATE
            customer_notes = VALUES(customer_notes),
            merchant_notes = VALUES(merchant_notes),
            driver_notes = VALUES(driver_notes),
            internal_notes = VALUES(internal_notes)
    """
    
    cursor.execute(query, notes)
    logger.debug(f"Upserted order notes for: {notes['order_id']}")


def upsert_order_metadata(cursor, metadata: Optional[Dict[str, Any]]) -> None:
    """Insert or update order metadata."""
    if not metadata or not metadata.get('order_id'):
        return
    
    query = """
        INSERT INTO order_metadata (
            order_id, source_platform, app_version, device_type, promo_code,
            is_first_order, customer_rating, customer_feedback, driver_rating, rated_at
        ) VALUES (
            %(order_id)s, %(source_platform)s, %(app_version)s, %(device_type)s,
            %(promo_code)s, %(is_first_order)s, %(customer_rating)s, %(customer_feedback)s,
            %(driver_rating)s, %(rated_at)s
        )
        ON DUPLICATE KEY UPDATE
            source_platform = VALUES(source_platform),
            app_version = VALUES(app_version),
            device_type = VALUES(device_type),
            promo_code = VALUES(promo_code),
            is_first_order = VALUES(is_first_order),
            customer_rating = VALUES(customer_rating),
            customer_feedback = VALUES(customer_feedback),
            driver_rating = VALUES(driver_rating),
            rated_at = VALUES(rated_at)
    """
    
    cursor.execute(query, metadata)
    logger.debug(f"Upserted order metadata for: {metadata['order_id']}")


def load_transformed_order(cursor, transformed_order: Dict[str, Any]) -> None:
    """
    Load a single transformed order into MySQL.
    Follows correct FK insert order:
    1. Dimension tables (customers, merchants, drivers, addresses)
    2. Payment & Tracking tables (referenced by orders FK)
    3. Fact table (orders)
    4. Child tables (items, actions, notes, metadata)
    """
    order_id = transformed_order['order']['order_id']
    
    logger.info(f"Loading order: {order_id}")
    
    # Step 1: Load dimension tables (no FK dependencies)
    upsert_customer(cursor, transformed_order.get('customer'))
    upsert_merchant(cursor, transformed_order.get('merchant'))
    upsert_driver(cursor, transformed_order.get('driver'))
    upsert_address(cursor, transformed_order.get('pickup_address'))
    upsert_address(cursor, transformed_order.get('dropoff_address'))
    
    # Step 2: Load payment & tracking BEFORE orders (orders has FK to these)
    upsert_payment(cursor, transformed_order.get('payment'))
    upsert_tracking(cursor, transformed_order.get('tracking'))
    
    # Step 3: Load fact table (orders) - NOW all FKs exist
    upsert_order(cursor, transformed_order['order'])
    
    # Step 4: Load child tables
    upsert_items(cursor, transformed_order.get('items', []))
    upsert_order_actions(cursor, transformed_order.get('actions', []))
    upsert_order_notes(cursor, transformed_order.get('notes'))
    upsert_order_metadata(cursor, transformed_order.get('metadata'))
    
    logger.info(f"✓ Successfully loaded order: {order_id}")


def load_orders(transformed_orders: List[Dict[str, Any]]) -> None:
    """
    Load multiple transformed orders into MySQL.
    Uses transactions - rolls back on any failure.
    """
    connection = None
    cursor = None
    
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        logger.info(f"Starting load of {len(transformed_orders)} orders")
        
        for transformed_order in transformed_orders:
            try:
                load_transformed_order(cursor, transformed_order)
            except Exception as e:
                logger.error(f"Failed to load order from {transformed_order.get('source_file')}: {str(e)}")
                raise
        
        # Commit transaction
        connection.commit()
        logger.info(f"✓ Successfully committed {len(transformed_orders)} orders to database")
        
    except Error as e:
        if connection:
            connection.rollback()
            logger.error(f"✗ Transaction rolled back due to error: {e}")
        raise
        
    except Exception as e:
        if connection:
            connection.rollback()
            logger.error(f"✗ Transaction rolled back due to error: {e}")
        raise
        
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    # Test database connection
    try:
        conn = get_db_connection()
        print("✓ Database connection successful")
        conn.close()
    except Exception as e:
        print(f"✗ Database connection failed: {str(e)}")
        print("\nMake sure these environment variables are set:")
        print("  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
