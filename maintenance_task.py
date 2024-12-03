import sqlite3
import argparse
import logging
from datetime import datetime, timedelta
import os
import shutil

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def perform_maintenance(db_path, dry_run):
    """
    Performs maintenance tasks such as vacuuming, analyzing, and reindexing the database.
    Deletes contests with less than 5 unique callsigns reporting.
    Additional maintenance tasks include cleaning up old backups, deleting old reports, and archiving old records.

    :param db_path: Path to the SQLite database.
    :param dry_run: If True, no changes are made to the database; just prints the deletions.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

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
                        # Delete related entries from contest_scores
                        cursor.execute("DELETE FROM contest_scores WHERE contest = ?", (contest,))
                        # Delete related entries from band_breakdown
                        cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE contest = ?)", (contest,))
                        # Delete related entries from qth_info
                        cursor.execute("DELETE FROM qth_info WHERE contest_score_id IN (SELECT id FROM contest_scores WHERE contest = ?)", (contest,))
                        logger.info(f"Deleted all entries related to contest '{contest}'")
                if not dry_run:
                    conn.commit()
                    logger.info("Database changes committed.")
            else:
                logger.info("No contests found with fewer than 5 unique callsigns.")

            # Delete contest data older than a specific threshold (e.g., 7 days)
            logger.info("Deleting contest data older than 7 days.")
            threshold_date = datetime.now() - timedelta(days=7)
            cursor.execute("SELECT id, contest FROM contest_scores WHERE timestamp < ?", (threshold_date,))
            old_contests = cursor.fetchall()

            if old_contests:
                for record_id, contest in old_contests:
                    logger.info(f"Deleting old contest record ID: {record_id}, Contest: {contest}")
                    if not dry_run:
                        # Delete related entries from contest_scores
                        cursor.execute("DELETE FROM contest_scores WHERE id = ?", (record_id,))
                        # Delete related entries from band_breakdown
                        cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id = ?", (record_id,))
                        # Delete related entries from qth_info
                        cursor.execute("DELETE FROM qth_info WHERE contest_score_id = ?", (record_id,))
                        logger.info(f"Deleted all entries related to contest record ID '{record_id}'")
                if not dry_run:
                    conn.commit()
                    logger.info("Old contest data deletion committed.")
            else:
                logger.info("No old contest data found to delete.")

            # Perform database maintenance
            if not dry_run:
                logger.info("Performing database maintenance (VACUUM, ANALYZE, REINDEX).")
                cursor.execute("VACUUM")
                cursor.execute("ANALYZE")
                cursor.execute("REINDEX")
                logger.info("Database maintenance completed.")
            else:
                logger.info("Dry-run mode: Skipping actual database maintenance.")

            # Additional maintenance tasks
            # Cleanup old backups
            backup_dir = "./backups"
            if os.path.exists(backup_dir):
                logger.info("Cleaning up old backups.")
                for filename in os.listdir(backup_dir):
                    file_path = os.path.join(backup_dir, filename)
                    if os.path.isfile(file_path):
                        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_age > timedelta(days=30):
                            if dry_run:
                                logger.info(f"Dry-run: Would delete old backup file: {file_path}")
                            else:
                                os.remove(file_path)
                                logger.info(f"Deleted old backup file: {file_path}")

            # Delete old reports
            reports_dir = "./reports"
            if os.path.exists(reports_dir):
                logger.info("Deleting old reports.")
                for filename in os.listdir(reports_dir):
                    file_path = os.path.join(reports_dir, filename)
                    if os.path.isfile(file_path):
                        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_age > timedelta(days=3):
                            if dry_run:
                                logger.info(f"Dry-run: Would delete old report file: {file_path}")
                            else:
                                os.remove(file_path)
                                logger.info(f"Deleted old report file: {file_path}")

            # Archive old records
            archive_dir = "./archive"
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)

            logger.info("Archiving old contest records.")
            cursor.execute("""
                SELECT id, contest, timestamp
                FROM contest_scores
                WHERE timestamp < ?
            """, (datetime.now() - timedelta(days=365),))
            old_records = cursor.fetchall()

            if old_records:
                for record_id, contest, timestamp in old_records:
                    archive_file = os.path.join(archive_dir, f"{contest}_{record_id}.txt")
                    if dry_run:
                        logger.info(f"Dry-run: Would archive record {record_id} for contest '{contest}'")
                    else:
                        with open(archive_file, 'w') as f:
                            f.write(f"Archived Record ID: {record_id}\nContest: {contest}\nTimestamp: {timestamp}\n")
                        cursor.execute("DELETE FROM contest_scores WHERE id = ?", (record_id,))
                        cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id = ?", (record_id,))
                        cursor.execute("DELETE FROM qth_info WHERE contest_score_id = ?", (record_id,))
                        logger.info(f"Archived and deleted record {record_id} for contest '{contest}'")
                if not dry_run:
                    conn.commit()
                    logger.info("Archiving completed and changes committed.")
            else:
                logger.info("No old records found to archive.")

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Maintenance Script for Contest Database.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--dry-run", action="store_true", help="Preview the changes without making any deletions or modifications.")
    args = parser.parse_args()

    logger.info(f"Starting maintenance script on database: {args.db}")
    logger.info(f"Dry-run mode: {'ON' if args.dry_run else 'OFF'}")
    
    perform_maintenance(args.db, args.dry_run)

    logger.info("Maintenance script finished.")
