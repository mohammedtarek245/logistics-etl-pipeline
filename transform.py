"""
transform.py
Applies vertical flattening, exploding, normalization, and data quality transformations.
Outputs clean Python data structures ready for MySQL loading.
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
import hashlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_datetime(dt_string: Optional[str]) -> Optional[str]:
    """
    Convert ISO 8601 datetime to MySQL DATETIME format.
    
    Args:
        dt_string: ISO 8601 datetime string or None
        
    Returns:
        MySQL formatted datetime string or None
    """
    if not dt_string:
        return None
    
    try:
        # Parse ISO 8601 and convert to MySQL format
        dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f"Failed to parse datetime '{dt_string}': {str(e)}")
        return None


def normalize_boolean(value: Any) -> Optional[bool]:
    """Convert various boolean representations to Python bool."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'y')
    if isinstance(value, int):
        return bool(value)
    return None


def generate_address_hash(address: Dict[str, Any]) -> str:
    """
    Generate deterministic hash for address deduplication.
    Based on: street + area + city + district + country + lat + lng
    """
    key_parts = [
        str(address.get('street', '')).strip().lower(),
        str(address.get('area', '')).strip().lower(),
        str(address.get('city', '')).strip().lower(),
        str(address.get('district', '')).strip().lower(),
        str(address.get('country', '')).strip().lower(),
        str(address.get('latitude', '')).strip(),
        str(address.get('longitude', '')).strip()
    ]
    
    key_string = '|'.join(key_parts)
    return hashlib.sha256(key_string.encode('utf-8')).hexdigest()[:32]


def transform_customer(customer_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Transform customer nested object to flat dictionary."""
    if not customer_data:
        return None
    
    return {
        'customer_id': customer_data.get('customer_id'),
        'first_name': customer_data.get('first_name'),
        'last_name': customer_data.get('last_name'),
        'phone': customer_data.get('phone'),
        'email': customer_data.get('email'),
        'is_verified': normalize_boolean(customer_data.get('is_verified'))
    }


def transform_merchant(merchant_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Transform merchant nested object to flat dictionary."""
    if not merchant_data:
        return None
    
    return {
        'merchant_id': merchant_data.get('merchant_id'),
        'business_name': merchant_data.get('business_name'),
        'contact_name': merchant_data.get('contact_name'),
        'phone': merchant_data.get('phone'),
        'email': merchant_data.get('email'),
        'category': merchant_data.get('category')
    }


def transform_driver(driver_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Transform driver nested object to flat dictionary."""
    if not driver_data:
        return None
    
    return {
        'driver_id': driver_data.get('driver_id'),
        'first_name': driver_data.get('first_name'),
        'last_name': driver_data.get('last_name'),
        'phone': driver_data.get('phone'),
        'vehicle_type': driver_data.get('vehicle_type'),
        'vehicle_plate': driver_data.get('vehicle_plate'),
        'rating': driver_data.get('rating'),
        'total_deliveries': driver_data.get('total_deliveries')
    }


def transform_address(address_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Transform address nested object to flat dictionary with generated ID."""
    if not address_data:
        return None
    
    address_id = generate_address_hash(address_data)
    
    return {
        'address_id': address_id,
        'floor': address_data.get('floor'),
        'apartment': address_data.get('apartment'),
        'building': address_data.get('building'),
        'street': address_data.get('street'),
        'area': address_data.get('area'),
        'city': address_data.get('city'),
        'district': address_data.get('district'),
        'governorate': address_data.get('governorate'),
        'postal_code': address_data.get('postal_code'),
        'country': address_data.get('country'),
        'country_code': address_data.get('country_code'),
        'latitude': address_data.get('latitude'),
        'longitude': address_data.get('longitude'),
        'landmark': address_data.get('landmark'),
        'special_instructions': address_data.get('special_instructions')
    }


def transform_items(items_data: Optional[List[Dict[str, Any]]], order_id: str) -> List[Dict[str, Any]]:
    """
    Explode items array into list of flat item dictionaries.
    Each item is linked to the order via order_id FK.
    """
    if not items_data:
        return []
    
    transformed_items = []
    
    for item in items_data:
        transformed_items.append({
            'item_id': item.get('item_id'),
            'order_id': order_id,
            'sku': item.get('sku'),
            'name': item.get('name'),
            'description': item.get('description'),
            'category': item.get('category'),
            'quantity': item.get('quantity'),
            'unit_price': item.get('unit_price'),
            'total_price': item.get('total_price'),
            'weight_kg': item.get('weight_kg'),
            'length_cm': item.get('length_cm'),
            'width_cm': item.get('width_cm'),
            'height_cm': item.get('height_cm')
        })
    
    return transformed_items


def transform_payment(payment_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Transform payment nested object to flat dictionary."""
    if not payment_data:
        return None
    
    return {
        'payment_id': payment_data.get('payment_id'),
        'payment_method': payment_data.get('payment_method'),
        'payment_status': payment_data.get('payment_status'),
        'currency': payment_data.get('currency'),
        'subtotal': payment_data.get('subtotal'),
        'delivery_fee': payment_data.get('delivery_fee'),
        'service_fee': payment_data.get('service_fee'),
        'discount_amount': payment_data.get('discount_amount'),
        'total_amount': payment_data.get('total_amount'),
        'collected_amount': payment_data.get('collected_amount'),
        'is_paid_back': normalize_boolean(payment_data.get('is_paid_back')),
        'collected_at': parse_datetime(payment_data.get('collected_at')),
        'business_collection_date': parse_datetime(payment_data.get('business_collection_date')),
        'business_collection_status': payment_data.get('business_collection_status')
    }


def transform_tracking(tracking_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Transform tracking nested object to flat dictionary."""
    if not tracking_data:
        return None
    
    return {
        'tracker_id': tracking_data.get('tracker_id'),
        'tracking_url': tracking_data.get('tracking_url'),
        'current_status': tracking_data.get('current_status'),
        'estimated_delivery_time': parse_datetime(tracking_data.get('estimated_delivery_time'))
    }


def transform_order_actions(actions_data: Optional[List[Dict[str, Any]]], order_id: str) -> List[Dict[str, Any]]:
    """
    Explode order_actions array into list of flat action dictionaries.
    Preserves ALL actions for audit trail.
    """
    if not actions_data:
        return []
    
    transformed_actions = []
    
    for action in actions_data:
        geo = action.get('geo_location', {}) or {}
        
        transformed_actions.append({
            'action_id': action.get('action_id'),
            'order_id': order_id,
            'action_type': action.get('action_type'),
            'status': action.get('status'),
            'timestamp': parse_datetime(action.get('timestamp')),
            'performed_by': action.get('performed_by'),
            'performed_by_id': action.get('performed_by_id'),
            'notes': action.get('notes'),
            'latitude': geo.get('latitude'),
            'longitude': geo.get('longitude'),
            'driver_id': action.get('driver_id'),
            'signature_url': action.get('signature_url'),
            'photo_url': action.get('photo_url'),
            'received_by': action.get('received_by')
        })
    
    return transformed_actions


def transform_order_notes(notes_data: Optional[Dict[str, Any]], order_id: str) -> Optional[Dict[str, Any]]:
    """Transform order notes to flat dictionary."""
    if not notes_data:
        return None
    
    return {
        'order_id': order_id,
        'customer_notes': notes_data.get('customer_notes'),
        'merchant_notes': notes_data.get('merchant_notes'),
        'driver_notes': notes_data.get('driver_notes'),
        'internal_notes': notes_data.get('internal_notes')
    }


def transform_order_metadata(metadata: Optional[Dict[str, Any]], order_id: str) -> Optional[Dict[str, Any]]:
    """Transform order metadata to flat dictionary."""
    if not metadata:
        return None
    
    return {
        'order_id': order_id,
        'source_platform': metadata.get('source_platform'),
        'app_version': metadata.get('app_version'),
        'device_type': metadata.get('device_type'),
        'promo_code': metadata.get('promo_code'),
        'is_first_order': normalize_boolean(metadata.get('is_first_order')),
        'customer_rating': metadata.get('customer_rating'),
        'customer_feedback': metadata.get('customer_feedback'),
        'driver_rating': metadata.get('driver_rating'),
        'rated_at': parse_datetime(metadata.get('rated_at'))
    }


def transform_order(order_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main transformation function.
    Applies vertical flattening and produces normalized data structures.
    
    Returns:
        Dictionary containing all transformed entities
    """
    order_id = order_data.get('order_id')
    
    if not order_id:
        raise ValueError("Order missing required field: order_id")
    
    logger.info(f"Transforming order: {order_id}")
    
    # Transform addresses first (needed for FK resolution)
    pickup_address = transform_address(order_data.get('pickup_address'))
    dropoff_address = transform_address(order_data.get('dropoff_address'))
    
    # Transform dimension entities
    customer = transform_customer(order_data.get('customer'))
    merchant = transform_merchant(order_data.get('merchant'))
    driver = transform_driver(order_data.get('driver'))
    
    # Transform payment and tracking (1:1 with order) - NO order_id parameter
    payment = transform_payment(order_data.get('payment'))
    tracking = transform_tracking(order_data.get('tracking'))
    
    # Transform fact table (orders)
    order = {
        'order_id': order_id,
        'order_number': order_data.get('order_number'),
        'order_type': order_data.get('order_type'),
        'order_status': order_data.get('order_status'),
        'created_at': parse_datetime(order_data.get('created_at')),
        'updated_at': parse_datetime(order_data.get('updated_at')),
        'scheduled_pickup_time': parse_datetime(order_data.get('scheduled_pickup_time')),
        'actual_pickup_time': parse_datetime(order_data.get('actual_pickup_time')),
        'scheduled_delivery_time': parse_datetime(order_data.get('scheduled_delivery_time')),
        'actual_delivery_time': parse_datetime(order_data.get('actual_delivery_time')),
        'customer_id': customer['customer_id'] if customer else None,
        'merchant_id': merchant['merchant_id'] if merchant else None,
        'driver_id': driver['driver_id'] if driver else None,
        'pickup_address_id': pickup_address['address_id'] if pickup_address else None,
        'dropoff_address_id': dropoff_address['address_id'] if dropoff_address else None,
        'payment_id': payment['payment_id'] if payment else None,
        'tracker_id': tracking['tracker_id'] if tracking else None
    }
    
    # Explode arrays
    items = transform_items(order_data.get('items'), order_id)
    actions = transform_order_actions(order_data.get('order_actions'), order_id)
    
    # Transform  tables
    notes = transform_order_notes(order_data.get('notes'), order_id)
    metadata = transform_order_metadata(order_data.get('metadata'), order_id)
    
    return {
        'order': order,
        'customer': customer,
        'merchant': merchant,
        'driver': driver,
        'pickup_address': pickup_address,
        'dropoff_address': dropoff_address,
        'items': items,
        'payment': payment,
        'tracking': tracking,
        'actions': actions,
        'notes': notes,
        'metadata': metadata
    }


def transform_orders(raw_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform multiple orders.
    
    Args:
        raw_orders: List of raw order dictionaries from extract
        
    Returns:
        List of transformed order structures
    """
    transformed = []
    
    for raw_order in raw_orders:
        try:
            file_name = raw_order.get('file_name', 'unknown')
            order_data = raw_order.get('data')
            
            logger.info(f"Processing file: {file_name}")
            transformed_order = transform_order(order_data)
            transformed_order['source_file'] = file_name
            transformed.append(transformed_order)
            
        except Exception as e:
            logger.error(f"Failed to transform order from {file_name}: {str(e)}")
            raise
    
    logger.info(f"Successfully transformed {len(transformed)} orders")
    return transformed


if __name__ == "__main__":
    # Test transformation with sample data
    sample_order = {
        'order_id': 'ORD-12345',
        'order_number': '12345',
        'order_type': 'delivery',
        'order_status': 'delivered',
        'created_at': '2020-08-17T14:53:28.122Z',
        'updated_at': '2020-08-17T16:20:00.000Z',
        'customer': {
            'customer_id': 'CUST-001',
            'first_name': 'John',
            'last_name': 'Doe',
            'phone': '+201234567890',
            'email': 'john@example.com',
            'is_verified': True
        },
        'merchant': {
            'merchant_id': 'MERCH-001',
            'business_name': 'Test Restaurant',
            'phone': '+201111111111'
        },
        'items': [
            {
                'item_id': 'ITEM-001',
                'name': 'Pizza',
                'quantity': 2,
                'unit_price': 100.0,
                'total_price': 200.0
            }
        ],
        'pickup_address': {
            'street': '123 Main St',
            'city': 'Cairo',
            'country': 'Egypt',
            'latitude': 30.0444,
            'longitude': 31.2357
        },
        'dropoff_address': {
            'street': '456 Side St',
            'city': 'Cairo',
            'country': 'Egypt',
            'latitude': 30.0500,
            'longitude': 31.2400
        }
    }
    
    try:
        result = transform_order(sample_order)
        print("✓ Transformation successful")
        print(f"  Order ID: {result['order']['order_id']}")
        print(f"  Items count: {len(result['items'])}")
        print(f"  Addresses: {result['pickup_address']['address_id']}, {result['dropoff_address']['address_id']}")
    except Exception as e:
        print(f"✗ Transformation failed: {str(e)}")