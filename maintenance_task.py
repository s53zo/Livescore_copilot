#!/usr/bin/env python3
import sqlite3
import argparse
import logging
from datetime import datetime, timedelta
import os
import shutil

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_qso_consistency(cursor):
    """
    Check QSO count consistency while accounting for stations that only report totals.
    Returns a tuple of (true_inconsistencies, total_without_breakdown)
    """
    cursor.execute("""
        WITH score_analysis AS (
            SELECT 
                cs.id,
                cs.callsign,
                cs.contest,
                cs.qsos as total_qsos,
                COUNT(bb.contest_score_id) as has_band_data,
                SUM(bb.qsos) as band_total
            FROM contest_scores cs
            LEFT JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            GROUP BY cs.id, cs.callsign, cs.contest, cs.qsos
        )
        SELECT 
            id, callsign, contest, total_qsos, band_total
        FROM score_analysis
        WHERE has_band_data > 0
        AND total_qsos != COALESCE(band_total, 0)
        AND total_qsos > 0
    """)
    true_inconsistencies = cursor.fetchall()

    cursor.execute("""
        SELECT COUNT(*) 
        FROM contest_scores cs
        WHERE NOT EXISTS (
            SELECT 1 
            FROM band_breakdown bb 
            WHERE bb.contest_score_id = cs.id
        )
        AND cs.qsos > 0
    """)
    total_without_breakdown = cursor.fetchone()[0]

    return true_inconsistencies, total_without_breakdown

def analyze_orphaned_records(cursor):
    """Analyze orphaned records to provide detailed information"""
    try:
        # Analyze orphaned band breakdown records
        cursor.execute("""
            SELECT bb.contest_score_id, 
                   COUNT(*) as record_count,
                   SUM(bb.qsos) as total_qsos,
                   GROUP_CONCAT(bb.band) as bands,
                   MIN(bb.qsos) as min_qsos,
                   MAX(bb.qsos) as max_qsos
            FROM band_breakdown bb 
            WHERE NOT EXISTS (
                SELECT 1 FROM contest_scores cs 
                WHERE cs.id = bb.contest_score_id
            )
            GROUP BY bb.contest_score_id
            ORDER BY total_qsos DESC
            LIMIT 10
        """)
        bb_analysis = cursor.fetchall()

        # Analyze orphaned QTH info records
        cursor.execute("""
            SELECT qi.contest_score_id,
                   qi.dxcc_country,
                   qi.continent,
                   qi.cq_zone,
                   qi.iaru_zone
            FROM qth_info qi 
            WHERE NOT EXISTS (
                SELECT 1 FROM contest_scores cs 
                WHERE cs.id = qi.contest_score_id
            )
            LIMIT 10
        """)
        qth_analysis = cursor.fetchall()

        return {'band_breakdown': bb_analysis, 'qth_info': qth_analysis}

    except Exception as e:
        logger.error(f"Error analyzing orphaned records: {e}")
        return None

def handle_orphaned_records(cursor, dry_run=True, threshold=1000):
    """
    Handle orphaned records with safeguards
    """
    # Get counts
    cursor.execute("SELECT COUNT(*) FROM band_breakdown bb WHERE NOT EXISTS (SELECT 1 FROM contest_scores cs WHERE cs.id = bb.contest_score_id)")
    orphaned_bb = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM qth_info qi WHERE NOT EXISTS (SELECT 1 FROM contest_scores cs WHERE cs.id = qi.contest_score_id)")
    orphaned_qth = cursor.fetchone()[0]

    if orphaned_bb > 0 or orphaned_qth > 0:
        logger.warning(f"Found orphaned records:")
        logger.warning(f"  Band breakdown: {orphaned_bb:,}")
        logger.warning(f"  QTH info: {orphaned_qth:,}")

        # Get sample analysis
        bb_analysis, qth_analysis = analyze_orphaned_records(cursor)

        logger.info("\nAnalysis of orphaned band breakdown records (top 10):")
        for record in bb_analysis:
            logger.info(f"Contest Score ID {record[0]}: {record[1]} records, {record[2]} QSOs")
            logger.info(f"  Bands: {record[3]}")
            logger.info(f"  QSO range: {record[4]} - {record[5]}")

        logger.info("\nAnalysis of orphaned QTH info records (top 10):")
        for record in qth_analysis:
            logger.info(f"Contest Score ID {record[0]}: {record[1]}, {record[2]}, CQ:{record[3]}, IARU:{record[4]}")

        # Safety check - if too many orphaned records, require manual confirmation
        if orphaned_bb > threshold or orphaned_qth > threshold:
            logger.warning("\nWARNING: Large number of orphaned records detected!")
            logger.warning("This might indicate a database issue that needs investigation.")
            logger.warning("Please run with --analyze-only first to review the analysis.")
            return False

        if not dry_run:
            logger.info("\nRemoving orphaned records...")
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")
            try:
                # Delete orphaned records
                cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                bb_deleted = cursor.rowcount
                
                cursor.execute("DELETE FROM qth_info WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                qth_deleted = cursor.rowcount
                
                cursor.execute("COMMIT")
                logger.info(f"Successfully removed {bb_deleted:,} band breakdown and {qth_deleted:,} QTH info orphaned records")
                return True
            except Exception as e:
                cursor.execute("ROLLBACK")
                logger.error(f"Error during orphaned record cleanup: {e}")
                return False
        else:
            logger.info("\nDry run - no records were deleted")
            return True
    else:
        logger.info("No orphaned records found")
        return True

def perform_maintenance(db_path, dry_run):
    """
    Performs enhanced maintenance tasks with improved database locking handling.
    Uses SQLite-compatible query patterns.
    """
    try:
        with sqlite3.connect(db_path, timeout=30) as conn:
            conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout
            cursor = conn.cursor()

            # 1. First perform read-only analysis
            logger.info("Checking for orphaned records...")
            orphaned_analysis = analyze_orphaned_records(cursor)
            if orphaned_analysis:
                logger.info("Analysis completed, proceeding with maintenance")

            # 2. Perform write operations in a single transaction
            if not dry_run:
                cursor.execute("BEGIN IMMEDIATE")  # Get exclusive lock
                try:
                    # Handle orphaned records cleanup
                    cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                    bb_deleted = cursor.rowcount
                    cursor.execute("DELETE FROM qth_info WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                    qth_deleted = cursor.rowcount
                    logger.info(f"Removed {bb_deleted} orphaned band records and {qth_deleted} orphaned QTH records")

                    # QSO Consistency Check
                    logger.info("Checking QSO count consistency...")
                    inconsistent_qsos, logs_without_breakdown = check_qso_consistency(cursor)
                    logger.info(f"Found {logs_without_breakdown} logs without band breakdown data (this is normal)")
                    if inconsistent_qsos:
                        logger.warning(f"Found {len(inconsistent_qsos)} records with QSO count mismatches")

                    # Clean up small contests
                    cursor.execute("""
                        SELECT contest, COUNT(DISTINCT callsign) as num_callsigns
                        FROM contest_scores
                        GROUP BY contest
                        HAVING num_callsigns < 5
                    """)
                    contests_to_delete = cursor.fetchall()

                    for contest, num_callsigns in contests_to_delete:
                        logger.info(f"Removing contest: {contest} ({num_callsigns} callsigns)")
                        # Get IDs first
                        cursor.execute("SELECT id FROM contest_scores WHERE contest = ?", (contest,))
                        contest_ids = [row[0] for row in cursor.fetchall()]
                        
                        # Delete related records
                        if contest_ids:
                            cursor.execute(f"DELETE FROM band_breakdown WHERE contest_score_id IN ({','.join('?' * len(contest_ids))})", 
                                         contest_ids)
                            cursor.execute(f"DELETE FROM qth_info WHERE contest_score_id IN ({','.join('?' * len(contest_ids))})", 
                                         contest_ids)
                            
                        # Delete main records
                        cursor.execute("DELETE FROM contest_scores WHERE contest = ?", (contest,))
                        logger.info(f"Deleted contest '{contest}' and all related records")

                    # Delete old records
                    threshold_date = datetime.now() - timedelta(days=3)
                    cursor.execute("SELECT id FROM contest_scores WHERE timestamp < ?", (threshold_date,))
                    old_ids = [row[0] for row in cursor.fetchall()]
                    
                    if old_ids:
                        placeholders = ','.join('?' * len(old_ids))
                        cursor.execute(f"DELETE FROM band_breakdown WHERE contest_score_id IN ({placeholders})", old_ids)
                        cursor.execute(f"DELETE FROM qth_info WHERE contest_score_id IN ({placeholders})", old_ids)
                        cursor.execute(f"DELETE FROM contest_scores WHERE id IN ({placeholders})", old_ids)
                        logger.info(f"Deleted {len(old_ids)} old contest records and related data")

                    cursor.execute("COMMIT")
                    logger.info("Database cleanup completed successfully")

                except Exception as e:
                    cursor.execute("ROLLBACK")
                    logger.error(f"Error during maintenance, rolling back: {e}")
                    raise

                # 3. Perform optimization as a separate operation
                try:
                    optimize_result = optimize_database(db_path)
                    if not optimize_result:
                        logger.warning("Database optimization skipped due to locks")
                except Exception as e:
                    logger.error(f"Optimization error (non-fatal): {e}")

            # 4. File System Maintenance
            backup_dir = "./backups"
            reports_dir = "./reports"
            archive_dir = "./archive"

            for directory in [backup_dir, reports_dir, archive_dir]:
                os.makedirs(directory, exist_ok=True)

            cleanup_old_files(backup_dir, 30, dry_run, "backup")
            cleanup_old_files(reports_dir, 3, dry_run, "report")

            if not dry_run:
                archive_old_records(cursor, archive_dir, conn)

            # 5. Final Statistics
            cursor.execute("SELECT COUNT(*) FROM contest_scores")
            total_scores = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT contest) FROM contest_scores")
            total_contests = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT callsign) FROM contest_scores")
            total_stations = cursor.fetchone()[0]

            # Get orphaned records count
            cursor.execute("SELECT COUNT(*) FROM band_breakdown WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
            orphaned_bb = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM qth_info WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
            orphaned_qth = cursor.fetchone()[0]

            logger.info("\nMaintenance Summary:")
            logger.info(f"Total Contests: {total_contests}")
            logger.info(f"Total Stations: {total_stations}")
            logger.info(f"Total Score Records: {total_scores}")
            logger.info(f"Orphaned Records Found: {orphaned_bb + orphaned_qth}")
            logger.info(f"Logs without Band Breakdown: {logs_without_breakdown}")
            logger.info(f"True QSO Inconsistencies: {len(inconsistent_qsos)}")

        return True

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        return False

def cleanup_old_files(directory, days, dry_run, file_type):
    """Helper function to clean up old files"""
    logger.info(f"Cleaning up old {file_type} files...")
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_age > timedelta(days=days):
                if dry_run:
                    logger.info(f"Would delete old {file_type} file: {file_path}")
                else:
                    os.remove(file_path)
                    logger.info(f"Deleted old {file_type} file: {file_path}")

def optimize_database(db_path):
    """Perform database optimization with retry logic"""
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # First run ANALYZE and REINDEX which can be in a transaction
            with sqlite3.connect(db_path, timeout=30) as conn:
                conn.execute("PRAGMA busy_timeout = 30000")
                logger.info("Running ANALYZE...")
                conn.execute("ANALYZE")
                logger.info("Running REINDEX...")
                conn.execute("REINDEX")
                
            # Now run VACUUM with a fresh connection
            with sqlite3.connect(db_path, timeout=30) as conn:
                logger.info("Running VACUUM...")
                conn.execute("VACUUM")
                return True
                
        except sqlite3.Error as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                logger.warning(f"Database locked, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            logger.error(f"Database optimization error: {e}")
            return False
            
    return False
        
def archive_old_records(cursor, archive_dir, conn):
    """Helper function to archive old records"""
    logger.info("Archiving old contest records...")
    cursor.execute("""
        SELECT id, contest, timestamp
        FROM contest_scores
        WHERE timestamp < ?
    """, (datetime.now() - timedelta(days=365),))
    old_records = cursor.fetchall()

    if old_records:
        for record_id, contest, timestamp in old_records:
            archive_file = os.path.join(archive_dir, f"{contest}_{record_id}.txt")
            with open(archive_file, 'w') as f:
                f.write(f"Archived Record ID: {record_id}\nContest: {contest}\nTimestamp: {timestamp}\n")
            cursor.execute("DELETE FROM contest_scores WHERE id = ?", (record_id,))
            cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id = ?", (record_id,))
            cursor.execute("DELETE FROM qth_info WHERE contest_score_id = ?", (record_id,))
            logger.info(f"Archived and deleted record {record_id} for contest '{contest}'")
        conn.commit()
        logger.info("Archiving completed")
    else:
        logger.info("No old records found to archive")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced Maintenance Script for Contest Database.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--dry-run", action="store_true", help="Preview the changes without making any deletions or modifications.")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze orphaned records without deletion.")
    parser.add_argument("--threshold", type=int, default=1000, help="Safety threshold for automatic orphaned record deletion.")
    args = parser.parse_args()

    logger.info(f"Starting maintenance script on database: {args.db}")
    logger.info(f"Dry-run mode: {'ON' if args.dry_run else 'OFF'}")
    
    perform_maintenance(args.db, args.dry_run)
    logger.info("Maintenance script finished.")
