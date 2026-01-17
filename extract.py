"""
extract.py
Reads JSON order files from a local directory.

"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_orders_from_directory(directory_path: str) -> List[Dict[Any, Any]]:
    """
    Extract all JSON order files from the specified directory.
    
    Args:
        directory_path: Path to directory containing JSON order files
        
    Returns:
        List of raw order dictionaries
        
    Raises:
        FileNotFoundError: If directory doesn't exist
        ValueError: If no JSON files found or JSON is invalid
    """
    dir_path = Path(directory_path)
    
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    
    if not dir_path.is_dir():
        raise ValueError(f"Path is not a directory: {directory_path}")
    
    json_files = list(dir_path.glob("*.json"))
    
    if not json_files:
        raise ValueError(f"No JSON files found in directory: {directory_path}")
    
    logger.info(f"Found {len(json_files)} JSON files in {directory_path}")
    
    orders = []
    
    for json_file in json_files:
        try:
            logger.info(f"Reading file: {json_file.name}")
            with open(json_file, 'r', encoding='utf-8') as f:
                order_data = json.load(f)
                orders.append({
                    'file_name': json_file.name,
                    'data': order_data
                })
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in file {json_file.name}: {str(e)}")
        except Exception as e:
            raise Exception(f"Error reading file {json_file.name}: {str(e)}")
    
    logger.info(f"Successfully extracted {len(orders)} orders")
    return orders


def validate_order_structure(order: Dict[Any, Any]) -> None:
    """
    Basic validation to ensure critical fields exist.
    
    Args:
        order: Raw order dictionary
        
    Raises:
        ValueError: If required fields are missing
    """
    required_fields = ['order_id', 'order_number', 'created_at']
    
    for field in required_fields:
        if field not in order:
            raise ValueError(f"Missing required field: {field}")
    
    logger.debug(f"Order {order['order_id']} passed validation")


if __name__ == "__main__":
    # Test extraction
    test_dir = r"C:\Users\Lenovo\Desktop\bosta\original JSON data"

    try:
        orders = extract_orders_from_directory(test_dir)
        print(f"\n✓ Successfully extracted {len(orders)} orders")
        for order in orders:
            validate_order_structure(order['data'])
            print(f"  - {order['file_name']}: Order ID {order['data']['order_id']}")
    except Exception as e:
        print(f"\n✗ Extraction failed: {str(e)}")