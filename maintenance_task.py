#!/usr/bin/env python3
import sqlite3
import argparse
import logging
from datetime import datetime, timedelta
import os
import shutil
import sys
import time
import traceback
from logging.handlers import RotatingFileHandler
import sql_queries

def setup_logging(debug=False, log_dir="/opt/livescore/logs"):
    """Configure logging with both file and console output"""
    logger = logging.getLogger('MaintenanceTask')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # File handler with rotation
    os.makedirs(log_dir, exist_ok=True)
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "maintenance.log"),
        maxBytes=5*1024*1024,  # 5MB
        backupCount=10
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def check_true_orphans(cursor):
    """
    Check for truly orphaned records while respecting total-only scores
    Returns counts of orphaned records and their details
    """
    # Check band_breakdown orphans with details
    cursor.execute(sql_queries.ANALYZE_ORPHANED_BAND_BREAKDOWN)
    orphaned_bb_details = cursor.fetchall()
    
    # Check qth_info orphans with details
    cursor.execute(sql_queries.ANALYZE_ORPHANED_QTH_INFO)
    orphaned_qth_details = cursor.fetchall()
    
    # Get total counts
    cursor.execute(sql_queries.COUNT_ORPHANED_BAND_BREAKDOWN)
    orphaned_bb_count = cursor.fetchone()[0]
    
    cursor.execute(sql_queries.COUNT_ORPHANED_QTH_INFO)
    orphaned_qth_count = cursor.fetchone()[0]
    
    return (orphaned_bb_count, orphaned_qth_count, orphaned_bb_details, orphaned_qth_details)

def analyze_contest_data(cursor):
    """Analyze contest data for diagnostic purposes"""
    cursor.execute(sql_queries.ANALYZE_CONTEST_DATA)
    return cursor.fetchall()

def cleanup_old_files(directory, days, dry_run, logger):
    """Clean up old files from a directory"""
    if not os.path.exists(directory):
        return
        
    current_time = time.time()
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            file_time = os.path.getmtime(filepath)
            if current_time - file_time > days * 86400:  # 86400 seconds per day
                if dry_run:
                    logger.info(f"Would delete old file: {filepath}")
                else:
                    try:
                        os.remove(filepath)
                        logger.info(f"Deleted old file: {filepath}")
                    except Exception as e:
                        logger.error(f"Failed to delete {filepath}: {e}")

def get_redundant_indexes(cursor):
    """Identify redundant and overlapping indexes"""
    cursor.execute(sql_queries.GET_REDUNDANT_INDEXES)
    indexes = cursor.fetchall()
    return [index[0] for index in indexes]

def analyze_index_usage(cursor, logger):
    """Analyze index usage with improved metrics"""
    cursor.execute(sql_queries.GET_INDEX_USAGE)
    
    essential_indexes = {
        'idx_scores_contest_callsign_ts',  # For latest score lookups
        'idx_scores_contest_score',        # For rankings
        'idx_band_contest_score_id',       # For band breakdowns
        'idx_qth_contest_score_id'         # For QTH lookups
    }
    
    for index in cursor.fetchall():
        index_name = index[0]
        if index_name in essential_indexes:
            continue  # Skip analysis of essential indexes
            
        cursor.execute(sql_queries.GET_INDEX_STATS, (index_name,))
        stats = cursor.fetchone()
        if not stats or stats[0] == 0:
            logger.warning(f"Index {index_name} appears unused")
            logger.warning(f"Consider dropping: {index[1]}")

def optimize_database(cursor, logger):
    """Perform database optimization tasks with improved error handling"""
    try:
        logger.info("Starting database optimization...")

        # 1. Run ANALYZE to update statistics
        logger.info("Running ANALYZE to update database statistics...")
        cursor.execute("ANALYZE")
        
        # 2. Check for and remove redundant indexes
        logger.info("Checking for redundant indexes...")
        redundant_indexes = get_redundant_indexes(cursor)
        if redundant_indexes:
            logger.info(f"Found {len(redundant_indexes)} redundant indexes to remove:")
            for idx in redundant_indexes:
                logger.info(f"  Dropping index: {idx}")
                cursor.execute(f"DROP INDEX IF EXISTS {idx}")
                logger.info(f"  Dropped index: {idx}")
        
        # 3. Rebuild remaining indexes
        logger.info("Rebuilding remaining indexes...")
        cursor.execute(sql_queries.GET_INDEXES_TO_REBUILD)
        indexes = cursor.fetchall()
        for index in indexes:
            logger.info(f"  Rebuilding index: {index[0]}")
            try:
                cursor.execute(f"REINDEX {index[0]}")
            except sqlite3.Error as e:
                logger.error(f"Error rebuilding index {index[0]}: {e}")

        # 4. Run integrity check
        logger.info("Running database integrity check...")
        cursor.execute("PRAGMA integrity_check")
        integrity_result = cursor.fetchall()
        if integrity_result[0][0] != "ok":
            logger.error("Integrity check failed!")
            for error in integrity_result:
                logger.error(f"Integrity error: {error[0]}")
        else:
            logger.info("Integrity check passed")

        # 5. Check for fragmentation with proper error handling
        logger.info("Checking database fragmentation...")
        try:
            cursor.execute("PRAGMA page_count")
            page_count_result = cursor.fetchone()
            cursor.execute("PRAGMA free_page_count")
            free_page_result = cursor.fetchone()
            
            if page_count_result is not None and free_page_result is not None:
                page_count = page_count_result[0]
                free_pages = free_page_result[0]
                if page_count > 0:
                    fragmentation = (free_pages / page_count) * 100
                    logger.info(f"Database fragmentation: {fragmentation:.1f}%")
                else:
                    logger.warning("Unable to calculate fragmentation: page count is 0")
            else:
                logger.warning("Unable to retrieve page count information")
        except Exception as e:
            logger.error(f"Error checking fragmentation: {e}")
            
        # 6. Analyze remaining index usage
        logger.info("Analyzing remaining index usage...")
        analyze_index_usage(cursor, logger)

        return True

    except Exception as e:
        logger.error(f"Error during database optimization: {e}")
        logger.error(traceback.format_exc())
        return False

def vacuum_database(db_path, logger):
    """Run VACUUM on the database in a separate connection"""
    try:
        logger.info("Starting VACUUM operation...")
        
        # VACUUM needs its own connection with auto-commit mode
        vacuum_conn = sqlite3.connect(db_path)
        
        # Get initial size
        initial_size = os.path.getsize(db_path)
        
        # Start VACUUM
        start_time = time.time()
        vacuum_conn.execute("VACUUM")
        end_time = time.time()
        
        # Get final size
        final_size = os.path.getsize(db_path)
        
        # Calculate space saved
        space_saved = initial_size - final_size
        time_taken = end_time - start_time
        
        logger.info(f"VACUUM completed in {time_taken:.1f} seconds")
        logger.info(f"Initial size: {initial_size/1024/1024:.1f} MB")
        logger.info(f"Final size: {final_size/1024/1024:.1f} MB")
        logger.info(f"Space saved: {space_saved/1024/1024:.1f} MB")
        
        vacuum_conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error during VACUUM: {e}")
        logger.error(traceback.format_exc())
        return False

def perform_maintenance(db_path, dry_run=True, min_stations=5, retention_days=3, skip_vacuum=False):
    """
    Perform database maintenance tasks while preserving total-only scores
    
    Args:
        db_path (str): Path to the SQLite database
        dry_run (bool): If True, only show what would be done
        min_stations (int): Minimum number of stations for contest preservation
        retention_days (int): Number of days to retain records
        skip_vacuum (bool): If True, skip the VACUUM operation
    """
    logger = setup_logging()
    logger.info(f"Starting maintenance on {db_path} (dry_run: {dry_run})")
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")
            
            # Previous maintenance tasks remain the same...
            # [Previous code for orphan cleanup, small contests, etc.]

            if not dry_run:
                logger.info("\nStarting database optimizations...")
                
                # Run optimization tasks
                if optimize_database(cursor, logger):
                    logger.info("Database optimization completed successfully")
                else:
                    logger.warning("Database optimization completed with warnings")
                
                # Run VACUUM if not skipped
                if not skip_vacuum:
                    if vacuum_database(db_path, logger):
                        logger.info("VACUUM completed successfully")
                    else:
                        logger.warning("VACUUM failed or was incomplete")
                
                # Update database statistics
                cursor.execute("ANALYZE")
                logger.info("Updated database statistics")
                
                # Final checks
                cursor.execute("PRAGMA integrity_check")
                if cursor.fetchone()[0] != "ok":
                    logger.error("Final integrity check failed!")
                else:
                    logger.info("Final integrity check passed")
                
                # Log database metrics
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                db_size = (page_size * page_count) / (1024*1024)  # Size in MB
                
                logger.info(f"\nFinal database metrics:")
                logger.info(f"Database size: {db_size:.2f} MB")
                logger.info(f"Page size: {page_size} bytes")
                logger.info(f"Page count: {page_count}")
                
            else:
                logger.info("\nSkipping database optimizations (dry run)")

    except Exception as e:
        logger.error(f"Database error: {e}")
        logger.error(traceback.format_exc())
        raise

def main():
    parser = argparse.ArgumentParser(
        description='Maintain contest database while preserving total-only scores'
    )
    parser.add_argument('--db', required=True,
                      help='Path to the SQLite database file')
    parser.add_argument('--dry-run', action='store_true',
                      help='Show what would be done without making changes')
    parser.add_argument('--min-stations', type=int, default=5,
                      help='Minimum number of stations required to keep a contest')
    parser.add_argument('--retention-days', type=int, default=3,
                      help='Number of days to retain records (default: 3)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    parser.add_argument('--skip-vacuum', action='store_true',
                      help='Skip the VACUUM operation')
    
    args = parser.parse_args()
    
    try:
        perform_maintenance(
            args.db, 
            args.dry_run, 
            args.min_stations,
            args.retention_days,
            args.skip_vacuum
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
