#!/usr/bin/env python3
import sqlite3
import logging
import time
import os
from datetime import datetime, timedelta
import traceback
import threading

class DatabaseMaintenance:
    """Database maintenance scheduler with integrated logging and task monitoring"""
    def __init__(self, db_path, log_path=None):
        self.db_path = db_path
        self.setup_logging(log_path)
        self._maintenance_thread = None
        self._stop_flag = False
        self.stats_path = os.path.join(os.path.dirname(log_path), 'maintenance_stats.json') if log_path else None
        self.last_cleanup_time = datetime.now()
        self.last_maintenance_time = datetime.now()

    def setup_logging(self, log_path=None):
        """Configure detailed logging"""
        self.logger = logging.getLogger('DatabaseMaintenance')
        self.logger.setLevel(logging.INFO)
        
        # Create formatters for different detail levels
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        basic_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Always add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(basic_formatter)
        console_handler.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)
        
        # Add file handler if log path is provided
        if log_path:
            try:
                # Create log directory if it doesn't exist
                os.makedirs(os.path.dirname(log_path), exist_ok=True)
                
                file_handler = logging.FileHandler(log_path)
                file_handler.setFormatter(detailed_formatter)
                file_handler.setLevel(logging.DEBUG)
                self.logger.addHandler(file_handler)
                
                self.logger.info(f"Maintenance logging initialized. Log file: {log_path}")
            except Exception as e:
                self.logger.error(f"Failed to setup file logging: {e}")

    def start(self):
        """Start the maintenance scheduler thread"""
        if not self._maintenance_thread or not self._maintenance_thread.is_alive():
            self._stop_flag = False
            self._maintenance_thread = threading.Thread(target=self.maintenance_worker)
            self._maintenance_thread.daemon = True
            self._maintenance_thread.start()
            self.logger.info("Maintenance scheduler started")
            return True
        return False

    def stop(self):
        """Stop the maintenance scheduler"""
        self._stop_flag = True
        if self._maintenance_thread:
            self._maintenance_thread.join()
            self.logger.info("Maintenance scheduler stopped")
        return True

    def is_maintenance_time(self):
        """Check if it's time for weekly maintenance (Thursday 3 AM)"""
        now = datetime.now()
        return (now.weekday() == 3 and  # Thursday
                now.hour == 3 and
                now.minute < 15)  # First 15 minutes of 3 AM

    def maintenance_worker(self):
        """Enhanced background worker that handles both regular maintenance and score cleanup"""
        self.logger.info("Maintenance worker started")
        
        while not self._stop_flag:
            try:
                now = datetime.now()
                
                # Log worker status periodically
                if now.minute == 0:  # Log status every hour
                    self.logger.info("Maintenance worker is running")
                    self.logger.info(f"Last cleanup: {self.last_cleanup_time}")
                    self.logger.info(f"Last maintenance: {self.last_maintenance_time}")
                
                # Check for score cleanup (every 15 minutes)
                if (now - self.last_cleanup_time).total_seconds() >= 10:  # 15 minutes
                    self.logger.info("Starting scheduled score cleanup")
                    try:
                        cleaned = self.cleanup_scores(minutes=90)
                        self.logger.info(f"Cleanup completed: {cleaned} records processed")
                        self.last_cleanup_time = now
                    except Exception as e:
                        self.logger.error(f"Error during cleanup: {e}")
                        self.logger.debug(traceback.format_exc())
                
                # Check for weekly maintenance (Thursday 3 AM)
                if self.is_maintenance_time() and (now - self.last_maintenance_time).days >= 1:
                    self.logger.info("Starting scheduled weekly maintenance")
                    try:
                        self.perform_maintenance()
                        self.last_maintenance_time = now
                    except Exception as e:
                        self.logger.error(f"Error during maintenance: {e}")
                        self.logger.debug(traceback.format_exc())
                
                # Sleep for 30 seconds between checks
                time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"Error in maintenance worker: {e}")
                self.logger.debug(traceback.format_exc())
                time.sleep(30)  # Sleep on error to prevent tight loop

    def cleanup_scores(self, minutes=90):
        """Cleanup old score records and remove duplicates"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # First remove duplicates
                self.logger.info("Starting duplicate record cleanup")
                cursor.execute("""
                    WITH duplicate_ids AS (
                        SELECT id
                        FROM contest_scores cs1
                        WHERE EXISTS (
                            SELECT 1 FROM contest_scores cs2
                            WHERE cs2.callsign = cs1.callsign
                            AND cs2.contest = cs1.contest
                            AND cs2.timestamp = cs1.timestamp
                            AND cs2.score = cs1.score
                            AND cs2.id < cs1.id
                        )
                    )
                    DELETE FROM contest_scores 
                    WHERE id IN (SELECT id FROM duplicate_ids)
                """)
                duplicates_removed = cursor.rowcount
                self.logger.info(f"Removed {duplicates_removed} duplicate records")
                
                # Get latest timestamps for each callsign/contest
                query = """
                    SELECT callsign, contest, MAX(timestamp) AS latest_timestamp
                    FROM contest_scores
                    GROUP BY callsign, contest
                """
                cursor.execute(query)
                latest_entries = cursor.fetchall()
                
                total_deleted = 0
                for entry_callsign, entry_contest, latest_timestamp in latest_entries:
                    latest_time = datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S')
                    cutoff_time = latest_time - timedelta(minutes=minutes)
                    
                    # Delete related records first
                    cursor.execute("""
                        DELETE FROM band_breakdown 
                        WHERE contest_score_id IN (
                            SELECT id FROM contest_scores 
                            WHERE callsign = ? AND contest = ? AND timestamp < ?
                        )
                    """, (entry_callsign, entry_contest, cutoff_time.strftime('%Y-%m-%d %H:%M:%S')))
                    
                    cursor.execute("""
                        DELETE FROM qth_info 
                        WHERE contest_score_id IN (
                            SELECT id FROM contest_scores 
                            WHERE callsign = ? AND contest = ? AND timestamp < ?
                        )
                    """, (entry_callsign, entry_contest, cutoff_time.strftime('%Y-%m-%d %H:%M:%S')))
                    
                    # Delete main scores
                    cursor.execute("""
                        DELETE FROM contest_scores 
                        WHERE callsign = ? AND contest = ? AND timestamp < ?
                    """, (entry_callsign, entry_contest, cutoff_time.strftime('%Y-%m-%d %H:%M:%S')))
                    
                    deleted_count = cursor.rowcount
                    total_deleted += deleted_count
                    if deleted_count > 0:
                        self.logger.debug(
                            f"Cleaned up {deleted_count} old records for "
                            f"{entry_callsign} in {entry_contest}"
                        )
                
                conn.commit()
                self.logger.info(
                    f"Score cleanup completed: {duplicates_removed} duplicates and "
                    f"{total_deleted} old records removed"
                )
                return total_deleted + duplicates_removed
                
        except Exception as e:
            self.logger.error(f"Error in score cleanup: {e}")
            self.logger.debug(traceback.format_exc())
            return 0

    def perform_maintenance(self):
        """Perform comprehensive database maintenance"""
        self.logger.info("Starting database maintenance")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Vacuum the database
                self.logger.info("Running VACUUM")
                cursor.execute("VACUUM")
                
                # Update statistics
                self.logger.info("Running ANALYZE")
                cursor.execute("ANALYZE")
                
                # Reindex the database
                self.logger.info("Reindexing database")
                cursor.execute("REINDEX")
                
                self.logger.info("Database maintenance completed successfully")
                
        except Exception as e:
            self.logger.error(f"Error during maintenance: {e}")
            self.logger.debug(traceback.format_exc())
            raise
