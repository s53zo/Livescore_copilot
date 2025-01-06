#!/usr/bin/env python3

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional
import sqlite3
from manticore_handler import ManticoreHandler

def setup_logging(debug: bool = False) -> logging.Logger:
    """Configure logging"""
    logger = logging.getLogger('ManticoreSync')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler('manticore_sync.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def perform_sync(sqlite_path: str, manticore_url: str, 
                batch_size: int = 1000, 
                logger: Optional[logging.Logger] = None) -> bool:
    """
    Perform initial synchronization between SQLite and Manticore
    
    Args:
        sqlite_path: Path to SQLite database
        manticore_url: URL to Manticore instance
        batch_size: Number of records to process in each batch
        logger: Logger instance
    
    Returns:
        bool: True if sync was successful, False otherwise
    """
    if logger is None:
        logger = setup_logging()
    
    try:
        handler = ManticoreHandler(manticore_url, sqlite_path)
        logger.info("Starting initial sync...")
        start_time = time.time()
        
        with sqlite3.connect(sqlite_path) as conn:
            # Get total count for progress tracking
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM contest_scores')
            total_records = cursor.fetchone()[0]
            
            logger.info(f"Found {total_records} records to sync")
            
            # Process in batches
            processed = 0
            cursor.execute('SELECT id FROM contest_scores ORDER BY id')
            
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                for (record_id,) in batch:
                    if handler.sync_record(record_id):
                        processed += 1
                    
                    # Log progress every 1000 records
                    if processed % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed
                        remaining = (total_records - processed) / rate if rate > 0 else 0
                        logger.info(
                            f"Processed {processed}/{total_records} records "
                            f"({processed/total_records*100:.1f}%) "
                            f"- {rate:.1f} records/sec "
                            f"- Est. {remaining/60:.1f} minutes remaining"
                        )
        
        elapsed = time.time() - start_time
        logger.info(
            f"Sync completed: {processed} records in {elapsed:.1f} seconds "
            f"({processed/elapsed:.1f} records/sec)"
        )
        return True
        
    except Exception as e:
        logger.error(f"Error during sync: {e}")
        logger.exception("Sync failed with exception:")
        return False

def main():
    parser = argparse.ArgumentParser(description='Sync SQLite database to Manticore')
    
    parser.add_argument('--db', required=True,
                      help='Path to SQLite database')
    parser.add_argument('--manticore-url', 
                      default='http://localhost:9308',
                      help='Manticore Search URL (default: http://localhost:9308)')
    parser.add_argument('--batch-size', type=int, default=1000,
                      help='Number of records to process in each batch (default: 1000)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Verify database exists
    if not Path(args.db).is_file():
        print(f"Error: Database file not found: {args.db}")
        sys.exit(1)
    
    logger = setup_logging(args.debug)
    
    if perform_sync(args.db, args.manticore_url, args.batch_size, logger):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
