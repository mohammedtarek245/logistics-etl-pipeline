"""
main.py
ETL Pipeline Orchestrator
Coordinates extract, transform,load operations and notification system.
"""

import os
import sys
import logging
from datetime import datetime
from notify import send_email
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import ETL modules
from extract import extract_orders_from_directory, validate_order_structure
from transform import transform_orders
from load import load_orders

# Configure logging
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(log_dir, f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def validate_environment():
    """
    Validate that required environment variables are set.
    """
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            f"Please set these before running the ETL pipeline."
        )
    
    logger.info("✓ Environment variables validated")


def run_etl_pipeline(json_directory: str):
    """
    Execute the complete ETL pipeline.
    """
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("ETL PIPELINE STARTED")
    logger.info(f"Source directory: {json_directory}")
    logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    try:
        # Validate environment
        validate_environment()
        
        # EXTRACT PHASE
        logger.info("\n--- EXTRACT PHASE ---")
        raw_orders = extract_orders_from_directory(json_directory)
        
        # Validate order structure
        for raw_order in raw_orders:
            validate_order_structure(raw_order['data'])
        
        logger.info(f"✓ Extracted {len(raw_orders)} orders")
        
        # TRANSFORM PHASE
        logger.info("\n--- TRANSFORM PHASE ---")
        transformed_orders = transform_orders(raw_orders)
        logger.info(f"✓ Transformed {len(transformed_orders)} orders")
        
        # LOAD PHASE
        logger.info("\n--- LOAD PHASE ---")
        load_orders(transformed_orders)
        logger.info(f"✓ Loaded {len(transformed_orders)} orders to database")
        
        # Success summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Orders processed: {len(transformed_orders)}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        print("Sending success notification email...")
        send_email(
            "ETL Pipeline Success",
            f"ETL Pipeline completed successfully.\n\n"
            f"Orders processed: {len(transformed_orders)}\n"
            f"Duration: {duration:.2f} seconds\n"
            f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        return True
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.error("\n" + "=" * 80)
        logger.error("ETL PIPELINE FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Duration before failure: {duration:.2f} seconds")
        logger.error("=" * 80)
        
        print("Sending failure notification email...")
        send_email(
            "ETL Pipeline Failure",
            f"ETL Pipeline failed with error:\n\n{str(e)}\n\n"
            f"Duration before failure: {duration:.2f} seconds"
        )
        
        # Re-raise to allow caller to handle
        raise


def main():
    """
    Main entry point for ETL pipeline.
    """
    print("\n" + "=" * 80)
    print("ORDER ETL PIPELINE")
    print("=" * 80)
    
    # Get directory path from command line or prompt user
    if len(sys.argv) > 1:
        json_directory = sys.argv[1]
    else:
        json_directory = input("\nEnter the directory path containing JSON order files: ").strip()
    
    if not json_directory:
        print("✗ No directory path provided")
        sys.exit(1)
    
    try:
        success = run_etl_pipeline(json_directory)
        if success:
            print("\n✓ ETL pipeline completed successfully!")
            print(f"  Log file: {log_filename}")
            sys.exit(0)
    except Exception as e:
        print(f"\n✗ ETL pipeline failed: {str(e)}")
        print(f"  Check log file for details: {log_filename}")
        sys.exit(1)


if __name__ == "__main__":
    main()