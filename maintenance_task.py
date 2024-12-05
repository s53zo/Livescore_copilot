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
    """
    Analyze orphaned records to provide detailed information about their origin
    """
    # Analyze orphaned band breakdown records
    cursor.execute("""
        SELECT bb.contest_score_id, 
               COUNT(*) as record_count,
               SUM(bb.qsos) as total_qsos,
               GROUP_CONCAT(DISTINCT bb.band) as bands,
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

    return bb_analysis, qth_analysis

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
    Performs enhanced maintenance tasks including data integrity checks,
    cleanup operations, and database optimization.

    :param db_path: Path to the SQLite database.
    :param dry_run: If True, no changes are made to the database; just prints the deletions.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # 0. Handle orphaned records with new function
            logger.info("Checking for orphaned records...")
            if not handle_orphaned_records(cursor, dry_run):
                logger.warning("Skipping orphaned record cleanup due to safety threshold")
            
            # 1. Data Integrity Checks
            logger.info("Performing data integrity checks...")
            
            # Check for orphaned records
            cursor.execute("SELECT COUNT(*) FROM band_breakdown bb WHERE NOT EXISTS (SELECT 1 FROM contest_scores cs WHERE cs.id = bb.contest_score_id)")
            orphaned_bb = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM qth_info qi WHERE NOT EXISTS (SELECT 1 FROM contest_scores cs WHERE cs.id = qi.contest_score_id)")
            orphaned_qth = cursor.fetchone()[0]

            if orphaned_bb > 0 or orphaned_qth > 0:
                logger.info(f"Found orphaned records - Band breakdown: {orphaned_bb}, QTH info: {orphaned_qth}")
                if not dry_run:
                    cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                    cursor.execute("DELETE FROM qth_info WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                    logger.info("Orphaned records removed")

            # 2. QSO Consistency Check
            logger.info("Checking QSO count consistency...")
            inconsistent_qsos, logs_without_breakdown = check_qso_consistency(cursor)
            logger.info(f"Found {logs_without_breakdown} logs without band breakdown data (this is normal)")
            if inconsistent_qsos:
                logger.warning(f"Found {len(inconsistent_qsos)} records with QSO count mismatches where band data exists")

            # 3. Original Cleanup Tasks
            # Fetch contests with less than 5 unique callsigns
            logger.info("Fetching contests with fewer than 5 unique callsigns.")
            cursor.execute("""
                SELECT contest, COUNT(DISTINCT callsign) as num_callsigns
                FROM contest_scores
                GROUP BY contest
                HAVING num_callsigns < 5
            """)
            contests_to_delete = cursor.fetchall()

            if contests_to_delete:
                logger.info(f"Found {len(contests_to_delete)} contests to delete.")
                for contest, num_callsigns in contests_to_delete:
                    logger.info(f"Contest: {contest}, Callsigns: {num_callsigns}")
                    if not dry_run:
                        cursor.execute("DELETE FROM contest_scores WHERE contest = ?", (contest,))
                        cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE contest = ?)", (contest,))
                        cursor.execute("DELETE FROM qth_info WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE contest = ?)", (contest,))
                        logger.info(f"Deleted all entries related to contest '{contest}'")

            # Delete old contest data (3 days)
            logger.info("Deleting contest data older than 3 days.")
            threshold_date = datetime.now() - timedelta(days=3)
            cursor.execute("SELECT id, contest FROM contest_scores WHERE timestamp < ?", (threshold_date,))
            old_contests = cursor.fetchall()

            if old_contests:
                for record_id, contest in old_contests:
                    if not dry_run:
                        cursor.execute("DELETE FROM contest_scores WHERE id = ?", (record_id,))
                        cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id = ?", (record_id,))
                        cursor.execute("DELETE FROM qth_info WHERE contest_score_id = ?", (record_id,))
                        logger.info(f"Deleted old contest record: {contest} (ID: {record_id})")

            # 4. Database Optimization
            if not dry_run:
                logger.info("Starting database optimization...")
                optimize_database(db_path)
            else:
                logger.info("Dry-run: Skipping database optimization")


            # 5. File System Maintenance
            backup_dir = "./backups"
            reports_dir = "./reports"
            archive_dir = "./archive"

            # Cleanup old backups (30 days)
            if os.path.exists(backup_dir):
                cleanup_old_files(backup_dir, 30, dry_run, "backup")

            # Cleanup old reports (3 days)
            if os.path.exists(reports_dir):
                cleanup_old_files(reports_dir, 3, dry_run, "report")

            # Archive old records (365 days)
            if not dry_run:
                os.makedirs(archive_dir, exist_ok=True)
                archive_old_records(cursor, archive_dir, conn)

            # 6. Gather and Log Statistics
            cursor.execute("SELECT COUNT(*) FROM contest_scores")
            total_scores = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT contest) FROM contest_scores")
            total_contests = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT callsign) FROM contest_scores")
            total_stations = cursor.fetchone()[0]

            logger.info("\nMaintenance Summary:")
            logger.info(f"Total Contests: {total_contests}")
            logger.info(f"Total Stations: {total_stations}")
            logger.info(f"Total Score Records: {total_scores}")
            logger.info(f"Orphaned Records Found: {orphaned_bb + orphaned_qth}")
            logger.info(f"Logs without Band Breakdown: {logs_without_breakdown}")
            logger.info(f"True QSO Inconsistencies: {len(inconsistent_qsos)}")

            if not dry_run:
                conn.commit()
                logger.info("All changes committed successfully")

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")

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
    """
    Perform database optimization operations with improved locking handling
    """
    max_retries = 3
    retry_delay = 5  # seconds
    current_try = 0
    
    while current_try < max_retries:
        try:
            # First run ANALYZE and REINDEX which can be in a transaction
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                logger.info("Running database optimizations...")
                
                cursor.execute("BEGIN IMMEDIATE")  # Get exclusive lock
                try:
                    cursor.execute("ANALYZE")
                    cursor.execute("REINDEX")
                    cursor.execute("COMMIT")
                    logger.info("ANALYZE and REINDEX completed")
                except:
                    cursor.execute("ROLLBACK")
                    raise

            # Now run VACUUM with a fresh connection and timeout
            with sqlite3.connect(db_path, timeout=30) as vacuum_conn:
                vacuum_conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout
                logger.info("Running VACUUM...")
                vacuum_conn.execute("VACUUM")
                logger.info("VACUUM completed")
                
            return True

        except sqlite3.Error as e:
            current_try += 1
            if "database is locked" in str(e):
                if current_try < max_retries:
                    logger.warning(f"Database locked, retry {current_try}/{max_retries} in {retry_delay} seconds")
                    time.sleep(retry_delay)
                    continue
            logger.error(f"Database optimization error: {e}")
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
