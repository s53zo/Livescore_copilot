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
    cursor.execute("""
        WITH latest_scores AS (
            SELECT cs.id, cs.callsign, cs.contest, cs.timestamp
            FROM contest_scores cs
            INNER JOIN (
                SELECT callsign, contest, MAX(timestamp) as max_ts
                FROM contest_scores
                GROUP BY callsign, contest
            ) latest ON cs.callsign = latest.callsign 
                AND cs.contest = latest.contest
                AND cs.timestamp = latest.max_ts
        )
        SELECT 
            cs.contest,
            COUNT(DISTINCT ls.callsign) as active_stations,
            COUNT(*) as total_records,
            COUNT(DISTINCT bb.contest_score_id) as records_with_bands,
            COUNT(DISTINCT qi.contest_score_id) as records_with_qth,
            MIN(cs.timestamp) as first_record,
            MAX(cs.timestamp) as last_record
        FROM contest_scores cs
        JOIN latest_scores ls ON cs.id = ls.id
        LEFT JOIN band_breakdown bb ON bb.contest_score_id = cs.id
        LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
        GROUP BY cs.contest
        ORDER BY active_stations DESC
    """)
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

def perform_maintenance(db_path, dry_run=True, min_stations=5, retention_days=3):
    """
    Perform database maintenance tasks while preserving total-only scores
    
    Args:
        db_path (str): Path to the SQLite database
        dry_run (bool): If True, only show what would be done
        min_stations (int): Minimum number of stations for contest preservation
        retention_days (int): Number of days to retain records
    """
    logger = setup_logging()
    logger.info(f"Starting maintenance on {db_path} (dry_run: {dry_run})")
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # 1. Initial database analysis
            logger.info("Analyzing database state...")
            contest_stats = analyze_contest_data(cursor)
            logger.info("\nCurrent contest statistics:")
            for stat in contest_stats:
                logger.info(f"Contest: {stat[0]}")
                logger.info(f"  Active Stations: {stat[1]}")
                logger.info(f"  Total Records: {stat[2]}")
                logger.info(f"  Records with band data: {stat[3]}")
                logger.info(f"  Records with QTH data: {stat[4]}")
                logger.info(f"  Time span: {stat[5]} to {stat[6]}")
            
            # 2. Check for true orphans
            orphaned_bb, orphaned_qth, bb_details, qth_details = check_true_orphans(cursor)
            logger.info(f"\nFound {orphaned_bb} orphaned band breakdown records")
            logger.info(f"Found {orphaned_qth} orphaned QTH info records")
            
            if orphaned_bb > 0:
                logger.info("\nBand breakdown orphans detail:")
                for record in bb_details:
                    logger.info(f"  Score ID {record[0]}: {record[1]} records, {record[2]} QSOs")
                    logger.info(f"    Bands: {record[3]}")
            
            if not dry_run:
                # Start transaction
                cursor.execute("BEGIN")
                try:
                    # 3. Clean up true orphans
                    if orphaned_bb > 0:
                        cursor.execute(sql_queries.DELETE_ORPHANED_BAND_BREAKDOWN)
                        logger.info(f"Deleted {orphaned_bb} orphaned band breakdown records")
                    
                    if orphaned_qth > 0:
                        cursor.execute(sql_queries.DELETE_ORPHANED_QTH_INFO)
                        logger.info(f"Deleted {orphaned_qth} orphaned QTH info records")
                    
                    # 4. Handle small contests
                    cursor.execute(sql_queries.FIND_SMALL_CONTESTS)
                    small_contests = cursor.fetchall()
                    
                    if small_contests:
                        for contest, num_stations in small_contests:
                            # Delete related records first
                            cursor.execute(sql_queries.DELETE_BAND_BREAKDOWN_BY_CONTEST_SCORE_ID, (contest,))
                            bb_deleted = cursor.rowcount
                            
                            cursor.execute(sql_queries.DELETE_QTH_INFO_BY_CONTEST_SCORE_ID, (contest,))
                            qth_deleted = cursor.rowcount
                            
                            cursor.execute(sql_queries.DELETE_CONTEST_SCORES_BY_CONTEST, (contest,))
                            cs_deleted = cursor.rowcount
                            
                            logger.info(f"Removed contest {contest} ({num_stations} stations):")
                            logger.info(f"  - {cs_deleted} score records")
                            logger.info(f"  - {bb_deleted} band breakdown records")
                            logger.info(f"  - {qth_deleted} QTH info records")
                    
                    # 5. Clean up old records
                    threshold_date = datetime.now() - timedelta(days=retention_days)
                    threshold_str = threshold_date.strftime('%Y-%m-%d %H:%M:%S')
                    
                    cursor.execute(sql_queries.GET_OLD_RECORDS, (threshold_str,))
                    old_records = cursor.fetchall()
                    
                    if old_records:
                        # Archive old records first
                        cursor.execute(sql_queries.GET_ARCHIVE_RECORDS, (threshold_str,))
                        records_to_archive = cursor.fetchall()
                        
                        archive_dir = "/opt/livescore/archive"
                        os.makedirs(archive_dir, exist_ok=True)
                        
                        for record_id, contest, timestamp in records_to_archive:
                            archive_file = os.path.join(archive_dir, f"{contest}_{record_id}.txt")
                            with open(archive_file, 'w') as f:
                                f.write(f"Archived Record ID: {record_id}\n")
                                f.write(f"Contest: {contest}\n")
                                f.write(f"Timestamp: {timestamp}\n")
                        
                        # Delete old records
                        cursor.execute(sql_queries.DELETE_BAND_BREAKDOWN_BY_CONTEST_SCORE_ID, (threshold_str,))
                        bb_deleted = cursor.rowcount
                        
                        cursor.execute(sql_queries.DELETE_QTH_INFO_BY_CONTEST_SCORE_ID, (threshold_str,))
                        qth_deleted = cursor.rowcount
                        
                        cursor.execute(sql_queries.DELETE_CONTEST_SCORES_BY_CONTEST, (threshold_str,))
                        cs_deleted = cursor.rowcount
                        
                        logger.info(f"\nArchived and cleaned up old records (before {threshold_str}):")
                        logger.info(f"  - {cs_deleted} score records")
                        logger.info(f"  - {bb_deleted} band breakdown records")
                        logger.info(f"  - {qth_deleted} QTH info records")
                    
                    # 6. Clean up old files
                    backup_dir = "/opt/livescore/backups"
                    reports_dir = "/opt/livescore/reports"
                    archive_dir = "/opt/livescore/archive"
                    
                    for directory in [backup_dir, reports_dir, archive_dir]:
                        cleanup_old_files(directory, retention_days, dry_run, logger)
                    
                    # Commit all changes
                    conn.commit()
                    logger.info("\nMaintenance completed successfully")
                    
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error during maintenance: {e}")
                    logger.error(traceback.format_exc())
                    raise
                
            else:
                logger.info("\nDry run completed - no changes made")
            
            # Final analysis
            if not dry_run:
                logger.info("\nFinal database state:")
                final_stats = analyze_contest_data(cursor)
                for stat in final_stats:
                    logger.info(f"Contest: {stat[0]}")
                    logger.info(f"  Active Stations: {stat[1]}")
                    logger.info(f"  Records with band data: {stat[3]}")
                    logger.info(f"  Time span: {stat[5]} to {stat[6]}")
    
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
    
    args = parser.parse_args()
    
    try:
        perform_maintenance(
            args.db, 
            args.dry_run, 
            args.min_stations,
            args.retention_days
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
