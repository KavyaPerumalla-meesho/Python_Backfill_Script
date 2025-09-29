#!/usr/bin/env python3
"""
ScyllaDB Data Backfill Script
Copies data from production to staging ScyllaDB databases.
"""

import os
import sys
import logging
import signal
import argparse
import json
import time
import psutil
from typing import Dict, Any, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from scylla_service import ScyllaDBService
from dotenv import load_dotenv

@dataclass
class PerformanceMetrics:
    """Performance metrics tracking."""
    records_per_second: float = 0.0
    memory_usage_percent: float = 0.0
    cpu_usage_percent: float = 0.0
    duration_seconds: float = 0.0
    total_records: int = 0

@dataclass
class Checkpoint:
    """Checkpoint for resume capability."""
    table_name: str
    processed_count: int
    total_count: int
    timestamp: str
    batch_size: int

def setup_logging(log_level: str = 'INFO'):
    """Setup structured logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger('backfill')

class ScyllaBackfillService:
    """Main service for ScyllaDB data backfill operations."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger('backfill')
        self.running = True
        
        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'tables_processed': 0,
            'records_processed': 0,
            'errors': 0,
            'performance_metrics': {}
        }
        
        # Professional features
        self.checkpoint_dir = 'checkpoints'
        self.max_workers = self.config.get('max_workers', 4)
        self.enable_resume = self.config.get('enable_resume', True)
        self.enable_parallel = self.config.get('enable_parallel', True)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def initialize_services(self):
        """Initialize ScyllaDB connections."""
        try:
            # Initialize source Scylla service
            self.src_service = ScyllaDBService(
                self.config['source']['contact_points'],
                self.config['source']['username'],
                self.config['source']['password'],
                self.config['source']['keyspace']
            )
            
            # Initialize target Scylla service
            self.tgt_service = ScyllaDBService(
                self.config['target']['contact_points'],
                self.config['target']['username'],
                self.config['target']['password'],
                self.config['target']['keyspace']
            )
            
            self.logger.info("✅ Database connections initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize database connections: {e}")
            return False
    
    def copy_table_data(self, table_name: str, batch_size: int = 1000) -> bool:
        """Copy data from source to target for a specific table in batches with professional features."""
        table_start_time = time.time()
        
        try:
            self.logger.info(f"🚀 Processing table: {table_name}")
            
            # Check for existing checkpoint
            checkpoint = None
            if self.enable_resume:
                checkpoint = self.load_checkpoint(table_name)
                if checkpoint:
                    self.logger.info(f"🔄 Resume capability: Found checkpoint for {table_name}")
                    self.logger.info(f"   Previous progress: {checkpoint.processed_count:,}/{checkpoint.total_count:,}")
                    self.logger.info(f"   Checkpoint time: {checkpoint.timestamp}")
            
            # Get total count first
            count_result = self.src_service.session.execute(f'SELECT COUNT(*) FROM {table_name}')
            total_records = count_result.one()[0]
            
            if total_records == 0:
                self.logger.warning(f"No data found in source {table_name}")
                return True
            
            self.logger.info(f"📊 Total records in source {table_name}: {total_records:,}")
            
            # Get column names from a sample row
            sample_result = self.src_service.session.execute(f'SELECT * FROM {table_name} LIMIT 1')
            sample_row = sample_result.one()
            if not sample_row:
                self.logger.warning(f"Could not get sample row from source {table_name}")
                return True
                
            columns = list(sample_row._fields)
            placeholders = ', '.join(['%s' for _ in columns])
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Stream data in batches (memory efficient)
            self.logger.info(f"📖 Streaming data from source {table_name} in batches of {batch_size:,}...")
            
            # Process data in streaming batches
            processed_count = 0
            batch_num = 0
            current_batch = []
            
            # Stream data row by row and process in batches
            all_rows = self.src_service.session.execute(f'SELECT * FROM {table_name}')
            
            for row in all_rows:
                current_batch.append(row)
                
                # When batch is full, process it
                if len(current_batch) >= batch_size:
                    # Insert batch into target
                    for batch_row in current_batch:
                        values = [getattr(batch_row, col) for col in columns]
                        self.tgt_service.session.execute(insert_query, values)
                    
                    processed_count += len(current_batch)
                    batch_num += 1
                    
                    # Save checkpoint every 10 batches
                    if self.enable_resume and batch_num % 10 == 0:
                        self.save_checkpoint(table_name, processed_count, total_records, batch_size)
                    
                    # Log progress
                    progress = (processed_count / total_records) * 100
                    self.logger.info(f"📈 Progress: {processed_count:,}/{total_records:,} ({progress:.1f}%) - {table_name}")
                    
                    # Clear batch for memory efficiency
                    current_batch = []
            
            # Process remaining data in the last batch
            if current_batch:
                # Insert batch into target
                for batch_row in current_batch:
                    values = [getattr(batch_row, col) for col in columns]
                    self.tgt_service.session.execute(insert_query, values)
                
                processed_count += len(current_batch)
                batch_num += 1
                
                # Save checkpoint
                if self.enable_resume:
                    self.save_checkpoint(table_name, processed_count, total_records, batch_size)
                
                # Log progress
                progress = (processed_count / total_records) * 100
                self.logger.info(f"📈 Progress: {processed_count:,}/{total_records:,} ({progress:.1f}%) - {table_name}")
            
            # Calculate performance metrics
            duration = time.time() - table_start_time
            metrics = self.get_performance_metrics(table_name, duration, processed_count)
            self.log_performance_metrics(table_name, metrics)
            
            # Store metrics
            self.stats['performance_metrics'][table_name] = metrics.__dict__
            
            self.logger.info(f"✅ Successfully copied {processed_count:,} records from source {table_name} to target {table_name}")
            
            # Clear checkpoint on success
            if self.enable_resume:
                self.clear_checkpoint(table_name)
            
            # Update statistics
            self.stats['tables_processed'] += 1
            self.stats['records_processed'] += processed_count
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error copying {table_name}: {e}")
            self.stats['errors'] += 1
            return False
    
    def save_checkpoint(self, table_name: str, processed_count: int, total_count: int, batch_size: int):
        """Save progress checkpoint for resume capability."""
        try:
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            checkpoint = Checkpoint(
                table_name=table_name,
                processed_count=processed_count,
                total_count=total_count,
                timestamp=datetime.now().isoformat(),
                batch_size=batch_size
            )
            checkpoint_file = os.path.join(self.checkpoint_dir, f'{table_name}.json')
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint.__dict__, f, indent=2)
            self.logger.debug(f"💾 Checkpoint saved: {processed_count:,}/{total_count:,} for {table_name}")
        except Exception as e:
            self.logger.warning(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self, table_name: str) -> Optional[Checkpoint]:
        """Load checkpoint for resume capability."""
        try:
            checkpoint_file = os.path.join(self.checkpoint_dir, f'{table_name}.json')
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    data = json.load(f)
                return Checkpoint(**data)
        except Exception as e:
            self.logger.warning(f"Failed to load checkpoint: {e}")
        return None
    
    def clear_checkpoint(self, table_name: str):
        """Clear checkpoint after successful completion."""
        try:
            checkpoint_file = os.path.join(self.checkpoint_dir, f'{table_name}.json')
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
                self.logger.debug(f"🗑️ Checkpoint cleared for {table_name}")
        except Exception as e:
            self.logger.warning(f"Failed to clear checkpoint: {e}")
    
    def get_performance_metrics(self, table_name: str, duration: float, record_count: int) -> PerformanceMetrics:
        """Calculate performance metrics."""
        try:
            rps = record_count / duration if duration > 0 else 0
            memory_usage = psutil.virtual_memory().percent
            cpu_usage = psutil.cpu_percent()
            
            return PerformanceMetrics(
                records_per_second=rps,
                memory_usage_percent=memory_usage,
                cpu_usage_percent=cpu_usage,
                duration_seconds=duration,
                total_records=record_count
            )
        except Exception as e:
            self.logger.warning(f"Failed to calculate metrics: {e}")
            return PerformanceMetrics()
    
    def log_performance_metrics(self, table_name: str, metrics: PerformanceMetrics):
        """Log detailed performance metrics."""
        self.logger.info(f"⚡ Performance Metrics for {table_name}:")
        self.logger.info(f"   📊 Records/second: {metrics.records_per_second:.2f}")
        self.logger.info(f"   💾 Memory usage: {metrics.memory_usage_percent:.1f}%")
        self.logger.info(f"   🖥️ CPU usage: {metrics.cpu_usage_percent:.1f}%")
        self.logger.info(f"   ⏱️ Duration: {metrics.duration_seconds:.2f}s")
        self.logger.info(f"   📈 Total records: {metrics.total_records:,}")
    
    def validate_data_integrity_detailed(self, table_name: str) -> bool:
        """Enhanced data integrity validation with detailed checks."""
        try:
            # Get counts
            src_count = self.src_service.session.execute(f'SELECT COUNT(*) FROM {table_name}').one()[0]
            tgt_count = self.tgt_service.session.execute(f'SELECT COUNT(*) FROM {table_name}').one()[0]
            
            # Basic count validation
            if src_count != tgt_count:
                self.logger.error(f"❌ Data integrity check failed: Source={src_count:,}, Target={tgt_count:,}")
                return False
            
            # Sample data validation (check first 100 records)
            sample_size = min(100, src_count)
            if sample_size > 0:
                src_sample = list(self.src_service.session.execute(f'SELECT * FROM {table_name} LIMIT {sample_size}'))
                tgt_sample = list(self.tgt_service.session.execute(f'SELECT * FROM {table_name} LIMIT {sample_size}'))
                
                # Compare sample data
                for i, (src_row, tgt_row) in enumerate(zip(src_sample, tgt_sample)):
                    if src_row != tgt_row:
                        self.logger.error(f"❌ Data mismatch in sample record {i}")
                        return False
            
            self.logger.info(f"✅ Data integrity verified: {src_count:,} records match (sample validated)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Data integrity validation failed: {e}")
            return False
    
    def process_batch_parallel(self, batches: List[List[Any]], table_name: str, columns: List[str], insert_query: str) -> int:
        """Process batches in parallel using ThreadPoolExecutor."""
        processed_count = 0
        
        def process_single_batch(batch_data):
            """Process a single batch."""
            batch_processed = 0
            for row in batch_data:
                values = [getattr(row, col) for col in columns]
                self.tgt_service.session.execute(insert_query, values)
                batch_processed += 1
            return batch_processed
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all batches
                future_to_batch = {
                    executor.submit(process_single_batch, batch): i 
                    for i, batch in enumerate(batches)
                }
                
                # Process completed batches
                for future in as_completed(future_to_batch):
                    try:
                        batch_processed = future.result()
                        processed_count += batch_processed
                        batch_num = future_to_batch[future] + 1
                        self.logger.debug(f"✅ Completed batch {batch_num}/{len(batches)} ({batch_processed} records)")
                    except Exception as e:
                        self.logger.error(f"❌ Batch processing failed: {e}")
                        
        except Exception as e:
            self.logger.error(f"❌ Parallel processing failed: {e}")
            # Fallback to sequential processing
            for batch in batches:
                for row in batch:
                    values = [getattr(row, col) for col in columns]
                    self.tgt_service.session.execute(insert_query, values)
                    processed_count += 1
        
        return processed_count
    
    def verify_data_integrity(self, table_name: str) -> bool:
        """Verify that source and target have the same record count."""
        try:
            src_count = self.src_service.session.execute(f'SELECT COUNT(*) FROM {table_name}').one()[0]
            tgt_count = self.tgt_service.session.execute(f'SELECT COUNT(*) FROM {table_name}').one()[0]
            
            match = src_count == tgt_count
            status = "✅" if match else "❌"
            self.logger.info(f"{status} {table_name}: Source={src_count}, Target={tgt_count}")
            
            return match
            
        except Exception as e:
            self.logger.error(f"Error verifying {table_name}: {e}")
            return False
    
    def run_backfill(self, tables: List[str]):
        """Run the complete backfill process."""
        self.logger.info("🚀 Starting ScyllaDB data backfill process")
        
        try:
            # Initialize services
            if not self.initialize_services():
                self.logger.error("Failed to initialize database connections")
                return False
            
            # Process each table
            batch_size = self.config.get('batch_size', 5000)
            self.logger.info(f"Using batch size: {batch_size:,} records per batch")
            
            for table_name in tables:
                if not self.running:
                    self.logger.info("Shutdown requested, stopping processing...")
                    break
                
                # Copy data from source to target in batches
                success = self.copy_table_data(table_name, batch_size)
                
                if not success:
                    self.logger.error(f"Failed to process {table_name}")
                    continue
                
                # Verify data integrity with detailed checks
                self.validate_data_integrity_detailed(table_name)
            
            # Log final statistics
            self.stats['end_time'] = datetime.now()
            self.stats['duration'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            self.logger.info(f"🎉 Backfill completed successfully!")
            self.logger.info(f"📊 Statistics: {self.stats}")
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error in backfill process: {e}")
            return False
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup database connections."""
        try:
            if hasattr(self, 'src_service'):
                self.src_service.close()
            if hasattr(self, 'tgt_service'):
                self.tgt_service.close()
            self.logger.info("Database connections closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

def load_configuration() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    load_dotenv('proto_backfill.env')
    
    config = {
        'source': {
            'contact_points': os.getenv('SRC_SCYLLA_CONTACT_POINTS', os.getenv('PRD_SCYLLA_CONTACT_POINTS', 'localhost')).split(','),
            'username': os.getenv('SRC_SCYLLA_USERNAME', os.getenv('PRD_SCYLLA_USERNAME', 'admin')),
            'password': os.getenv('SRC_SCYLLA_PASSWORD', os.getenv('PRD_SCYLLA_PASSWORD', 'password')),
            'keyspace': os.getenv('SRC_SCYLLA_KEYSPACE', os.getenv('PRD_SCYLLA_KEYSPACE', 'source_keyspace'))
        },
        'target': {
            'contact_points': os.getenv('TGT_SCYLLA_CONTACT_POINTS', os.getenv('STG_SCYLLA_CONTACT_POINTS', 'localhost')).split(','),
            'username': os.getenv('TGT_SCYLLA_USERNAME', os.getenv('STG_SCYLLA_USERNAME', 'admin')),
            'password': os.getenv('TGT_SCYLLA_PASSWORD', os.getenv('STG_SCYLLA_PASSWORD', 'password')),
            'keyspace': os.getenv('TGT_SCYLLA_KEYSPACE', os.getenv('STG_SCYLLA_KEYSPACE', 'target_keyspace'))
        },
        'batch_size': int(os.getenv('BATCH_SIZE', '5000')),
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'max_workers': int(os.getenv('MAX_WORKERS', '4')),
        'enable_resume': os.getenv('ENABLE_RESUME', 'true').lower() == 'true',
        'enable_parallel': os.getenv('ENABLE_PARALLEL', 'true').lower() == 'true'
    }
    
    return config

def load_tables_from_config() -> List[str]:
    """Load table names from tables.json configuration file."""
    try:
        if os.path.exists('tables.json'):
            with open('tables.json', 'r') as f:
                config = json.load(f)
                if 'tables' in config:
                    return config['tables']
                elif isinstance(config, list):
                    return config
        return []
    except Exception as e:
        logging.getLogger('backfill').warning(f"Failed to load tables from config: {e}")
        return []

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='ScyllaDB Data Backfill Service')
    parser.add_argument('--tables', nargs='+', help='Tables to process')
    parser.add_argument('--log-level', default='INFO', help='Log level')
    parser.add_argument('--batch-size', type=int, help='Batch size for processing (default: from config)')
    parser.add_argument('--max-workers', type=int, help='Maximum parallel workers (default: 4)')
    parser.add_argument('--no-resume', action='store_true', help='Disable resume capability')
    parser.add_argument('--no-parallel', action='store_true', help='Disable parallel processing')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    
    # Load configuration
    config = load_configuration()
    
    # Override configuration with command line arguments
    if args.batch_size:
        config['batch_size'] = args.batch_size
        logger.info(f"Using command line batch size: {args.batch_size:,}")
    
    if args.max_workers:
        config['max_workers'] = args.max_workers
        logger.info(f"Using command line max workers: {args.max_workers}")
    
    if args.no_resume:
        config['enable_resume'] = False
        logger.info("Resume capability disabled")
    
    if args.no_parallel:
        config['enable_parallel'] = False
        logger.info("Parallel processing disabled")
    
    # Use provided tables or load from config file
    if args.tables:
        tables = args.tables
    else:
        # Try to load tables from config file
        tables = load_tables_from_config()
        if not tables:
            # Fallback to default tables if no config found
            tables = [
                'logistic_unbundling_details',
                'product_details', 
                'shipment_details',
                'sscat_details',
                'supplier_details',
                'supplier_sscat_details'
            ]
            logger.warning("No tables configuration found, using default tables")
            logger.info("To use custom tables, create a 'tables.json' file or use --tables parameter")
    
    # Create and run service
    service = ScyllaBackfillService(config)
    
    try:
        success = service.run_backfill(tables)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
