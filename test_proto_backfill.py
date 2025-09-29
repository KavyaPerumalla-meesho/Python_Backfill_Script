#!/usr/bin/env python3
"""
Test script for ScyllaDB backfill functionality.
"""

import os
import sys
import logging
from dotenv import load_dotenv
from scylla_service import ScyllaDBService

# Load environment variables
load_dotenv('proto_backfill.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_database_connections():
    """Test connections to both databases."""
    try:
        logger.info("Testing database connections...")
        
        # Test PRD connection
        prd_service = ScyllaDBService(
            contact_points=[os.getenv('PRD_SCYLLA_CONTACT_POINTS')],
            username=os.getenv('PRD_SCYLLA_USERNAME'),
            password=os.getenv('PRD_SCYLLA_PASSWORD'),
            keyspace=os.getenv('PRD_SCYLLA_KEYSPACE')
        )
        
        # Test STG connection
        stg_service = ScyllaDBService(
            contact_points=[os.getenv('STG_SCYLLA_CONTACT_POINTS')],
            username=os.getenv('STG_SCYLLA_USERNAME'),
            password=os.getenv('STG_SCYLLA_PASSWORD'),
            keyspace=os.getenv('STG_SCYLLA_KEYSPACE')
        )
        
        # Test table access
        tables = ['logistic_unbundling_details', 'product_details', 'shipment_details', 
                 'sscat_details', 'supplier_details', 'supplier_sscat_details']
        
        logger.info("Checking PRD tables...")
        for table in tables:
            try:
                result = prd_service.session.execute(f'SELECT COUNT(*) FROM {table}')
                count = result.one()[0]
                logger.info(f"PRD {table}: {count} records")
            except Exception as e:
                logger.error(f"Error reading PRD {table}: {e}")
        
        logger.info("Checking STG tables...")
        for table in tables:
            try:
                result = stg_service.session.execute(f'SELECT COUNT(*) FROM {table}')
                count = result.one()[0]
                logger.info(f"STG {table}: {count} records")
            except Exception as e:
                logger.error(f"Error reading STG {table}: {e}")
        
        # Close connections
        prd_service.close()
        stg_service.close()
        
        logger.info("✅ All database connections successful!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

def test_backfill_simulation():
    """Test a small backfill simulation."""
    try:
        logger.info("Running backfill simulation...")
        
        # Import and run the main script
        from proto_backfill_main import ScyllaBackfillService, load_configuration
        
        config = load_configuration()
        service = ScyllaBackfillService(config)
        
        # Test with just one table
        test_tables = ['logistic_unbundling_details']
        success = service.run_backfill(test_tables)
        
        if success:
            logger.info("✅ Backfill simulation successful!")
        else:
            logger.error("❌ Backfill simulation failed!")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Backfill simulation failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("SCYLLADB BACKFILL TEST")
    print("=" * 60)
    
    # Test connections first
    if test_database_connections():
        print("\n" + "=" * 60)
        print("RUNNING BACKFILL SIMULATION")
        print("=" * 60)
        test_backfill_simulation()
    else:
        print("❌ Connection test failed. Please check your configuration.")
        sys.exit(1)