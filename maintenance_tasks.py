import argparse
from datetime import datetime, timedelta
import threading
import time
import logging
import sqlite3
import os
import json
import sys

class DatabaseMaintenance:
    """Database maintenance scheduler that can be integrated with the main server"""
    def __init__(self, db_path, log_path=None):
        self.db_path = db_path
        self.setup_logging(log_path)
        self._maintenance_thread = None
        self._stop_flag = False
        self.stats_path = os.path.join(os.path.dirname(log_path), 'maintenance_stats.json') if log_path else None
        
    def setup_logging(self, log_path=None):
        """Configure logging to both file and console"""
        self.logger = logging.getLogger('DatabaseMaintenance')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (if log path is provided)
        if log_path:
            try:
                os.makedirs(os.path.dirname(log_path), exist_ok=True)
                file_handler = logging.FileHandler(log_path)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                self.logger.error(f"Error setting up log file: {e}")

    def start(self):
        """Start the maintenance scheduler"""
        self._stop_flag = False
        self._maintenance_thread = threading.Thread(
            target=self.maintenance_worker,
            daemon=True
        )
        self._maintenance_thread.start()
        self.logger.info("Maintenance scheduler started")
    
    def stop(self):
        """Stop the maintenance scheduler"""
        self._stop_flag = True
        if self._maintenance_thread:
            self._maintenance_thread.join(timeout=60)
        self.logger.info("Maintenance scheduler stopped")

    def is_maintenance_time(self):
        """Check if it's time for maintenance (Thursday 3 AM)"""
        now = datetime.now()
        return now.weekday() == 3 and now.hour == 3 and now.minute == 0

    def cleanup_small_contests(self, min_participants=10):
        """Remove contests with fewer than specified participants"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Find contests to remove
                cursor.execute("""
                    WITH participant_counts AS (
                        SELECT contest, COUNT(DISTINCT callsign) as count
                        FROM contest_scores
                        GROUP BY contest
                        HAVING count < ?
                    )
                    SELECT contest, count FROM participant_counts
                """, (min_participants,))
                
                contests_to_remove = cursor.fetchall()
                
                if contests_to_remove:
                    self.logger.info(f"Found {len(contests_to_remove)} contests with < {min_participants} participants")
                    for contest, count in contests_to_remove:
                        self.logger.info(f"Removing contest {contest} with {count} participants")
                        
                        # Delete related records first
                        cursor.execute("""
                            DELETE FROM band_breakdown 
                            WHERE contest_score_id IN (
                                SELECT id FROM contest_scores WHERE contest = ?
                            )
                        """, (contest,))
                        
                        cursor.execute("""
                            DELETE FROM qth_info 
                            WHERE contest_score_id IN (
                                SELECT id FROM contest_scores WHERE contest = ?
                            )
                        """, (contest,))
                        
                        # Delete main contest records
                        cursor.execute("DELETE FROM contest_scores WHERE contest = ?", (contest,))
                        
                    conn.commit()
                    self.logger.info("Small contests cleanup completed")
                else:
                    self.logger.info(f"No contests found with fewer than {min_participants} participants")
                    
        except Exception as e:
            self.logger.error(f"Error in cleanup_small_contests: {e}")

    def cleanup_old_records(self, days=3):
        """Remove records older than specified number of days"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
                
                # Delete related records first
                cursor.execute("""
                    DELETE FROM band_breakdown 
                    WHERE contest_score_id IN (
                        SELECT id FROM contest_scores WHERE timestamp < ?
                    )
                """, (cutoff_date,))
                bb_count = cursor.rowcount
                
                cursor.execute("""
                    DELETE FROM qth_info 
                    WHERE contest_score_id IN (
                        SELECT id FROM contest_scores WHERE timestamp < ?
                    )
                """, (cutoff_date,))
                qth_count = cursor.rowcount
                
                # Delete main records
                cursor.execute("DELETE FROM contest_scores WHERE timestamp < ?", (cutoff_date,))
                cs_count = cursor.rowcount
                
                conn.commit()
                self.logger.info(f"Removed {cs_count} scores, {bb_count} band records, {qth_count} QTH records")
                
        except Exception as e:
            self.logger.error(f"Error in cleanup_old_records: {e}")

    def vacuum_database(self):
        """Perform VACUUM operation"""
        try:
            self.logger.info("Starting VACUUM operation")
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("VACUUM")
            self.logger.info("VACUUM completed")
        except Exception as e:
            self.logger.error(f"Error in vacuum_database: {e}")

    def reindex_database(self):
        """Rebuild all indexes"""
        try:
            self.logger.info("Starting database reindexing")
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get list of all indexes
                cursor.execute("""
                    SELECT name, tbl_name 
                    FROM sqlite_master 
                    WHERE type = 'index'
                """)
                
                for index_name, table_name in cursor.fetchall():
                    self.logger.info(f"Reindexing {index_name} on {table_name}")
                    cursor.execute(f"REINDEX {index_name}")
                
            self.logger.info("Reindexing completed")
        except Exception as e:
            self.logger.error(f"Error in reindex_database: {e}")

    def maintenance_worker(self):
      """Background worker that performs maintenance at the scheduled time"""
      while not self._stop_flag:
          try:
              now = datetime.now()
              
              if self.is_maintenance_time():
                  self.perform_maintenance()
                  # Sleep until next minute to avoid multiple runs
                  time.sleep(60)
              else:
                  # Sleep for 30 seconds between checks
                  time.sleep(30)
                  
          except Exception as e:
              self.logger.error(f"Error in maintenance worker: {e}")
              time.sleep(30)
    
    def run_maintenance_now(self):
        """Run maintenance immediately, outside of the normal schedule"""
        self.logger.info("Starting manual maintenance run")
        self.perform_maintenance()
        self.logger.info("Manual maintenance completed")

    def check_database_integrity(self):
        """Perform comprehensive database integrity checks"""
        self.logger.info("Starting database integrity checks")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 1. Check SQLite database integrity
                self.logger.info("Checking SQLite integrity")
                cursor.execute("PRAGMA integrity_check")
                integrity_result = cursor.fetchone()[0]
                if integrity_result != "ok":
                    self.logger.error(f"Database integrity check failed: {integrity_result}")
                    return False

                # 2. Check for orphaned records
                self.logger.info("Checking for orphaned records")
                cursor.execute("""
                    SELECT COUNT(*) FROM band_breakdown 
                    WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)
                """)
                orphaned_bb = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM qth_info 
                    WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)
                """)
                orphaned_qth = cursor.fetchone()[0]

                if orphaned_bb > 0 or orphaned_qth > 0:
                    self.logger.warning(f"Found orphaned records: {orphaned_bb} band_breakdown, {orphaned_qth} qth_info")
                    # Clean up orphaned records
                    cursor.execute("DELETE FROM band_breakdown WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                    cursor.execute("DELETE FROM qth_info WHERE contest_score_id NOT IN (SELECT id FROM contest_scores)")
                    conn.commit()
                    self.logger.info("Cleaned up orphaned records")

                # 3. Check for data consistency
                self.logger.info("Checking data consistency")
                cursor.execute("""
                    SELECT cs.id, cs.callsign, cs.contest, cs.qsos,
                           (SELECT SUM(bb.qsos) FROM band_breakdown bb WHERE bb.contest_score_id = cs.id)
                    FROM contest_scores cs
                    WHERE cs.qsos != (SELECT SUM(bb.qsos) FROM band_breakdown bb WHERE bb.contest_score_id = cs.id)
                    OR cs.qsos IS NULL
                """)
                inconsistent_qsos = cursor.fetchall()
                
                if inconsistent_qsos:
                    self.logger.warning(f"Found {len(inconsistent_qsos)} records with QSO count inconsistencies")
                    for record in inconsistent_qsos:
                        self.logger.warning(f"ID: {record[0]}, Call: {record[1]}, Contest: {record[2]}, "
                                          f"Score QSOs: {record[3]}, Band QSOs: {record[4]}")

                # 4. Check foreign key integrity
                self.logger.info("Checking foreign key integrity")
                cursor.execute("PRAGMA foreign_key_check")
                fk_violations = cursor.fetchall()
                if fk_violations:
                    self.logger.error(f"Found {len(fk_violations)} foreign key violations")
                    return False

                # 5. Save integrity check stats
                self.save_integrity_stats({
                    'timestamp': datetime.now().isoformat(),
                    'integrity_check': integrity_result == "ok",
                    'orphaned_records_cleaned': orphaned_bb + orphaned_qth,
                    'inconsistent_qsos': len(inconsistent_qsos)
                })

                self.logger.info("Database integrity checks completed successfully")
                return True

        except Exception as e:
            self.logger.error(f"Error during integrity check: {e}")
            return False

    def optimize_performance(self):
        """Perform database performance optimization tasks"""
        self.logger.info("Starting performance optimization")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 1. Update database statistics
                self.logger.info("Updating database statistics")
                cursor.execute("ANALYZE")
                
                # 2. Check and optimize indexes
                self.logger.info("Analyzing index usage")
                cursor.execute("""
                    SELECT name, sql FROM sqlite_master 
                    WHERE type='index' AND sql IS NOT NULL
                """)
                indexes = cursor.fetchall()
                
                for index_name, index_sql in indexes:
                    # Check index usage statistics
                    cursor.execute(f"ANALYZE sqlite_master")
                    cursor.execute(f"""
                        SELECT stat FROM sqlite_stat1 
                        WHERE idx = ?
                    """, (index_name,))
                    stat = cursor.fetchone()
                    
                    if stat:
                        self.logger.info(f"Index {index_name} usage stats: {stat[0]}")
                
                # 3. Optimize database page size and cache
                self.logger.info("Optimizing database configuration")
                cursor.execute("PRAGMA page_size")
                current_page_size = cursor.fetchone()[0]
                
                if current_page_size < 4096:
                    self.logger.info("Optimizing page size")
                    cursor.execute("PRAGMA page_size = 4096")
                
                cursor.execute("PRAGMA cache_size = -2000") # Set to 2MB
                cursor.execute("PRAGMA temp_store = MEMORY")
                cursor.execute("PRAGMA mmap_size = 30000000000") # 30GB
                cursor.execute("PRAGMA journal_mode = WAL")
                
                # 4. Check for missing indexes on commonly queried columns
                self.logger.info("Checking for missing indexes")
                common_queries = [
                    ("contest_scores", "timestamp"),
                    ("contest_scores", "contest,timestamp"),
                    ("contest_scores", "callsign,contest"),
                    ("band_breakdown", "contest_score_id,band")
                ]
                
                for table, columns in common_queries:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM sqlite_master 
                        WHERE type='index' AND sql LIKE '%{table}%{columns}%'
                    """)
                    if cursor.fetchone()[0] == 0:
                        self.logger.warning(f"Missing potentially useful index on {table}({columns})")

                # 5. Monitor and log performance metrics
                self.logger.info("Gathering performance metrics")
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                db_size = (page_count * page_size) / (1024*1024) # Size in MB
                
                # Save performance stats
                self.save_performance_stats({
                    'timestamp': datetime.now().isoformat(),
                    'database_size_mb': db_size,
                    'page_size': page_size,
                    'page_count': page_count
                })

                self.logger.info("Performance optimization completed")
                return True

        except Exception as e:
            self.logger.error(f"Error during performance optimization: {e}")
            return False

    def save_integrity_stats(self, stats):
        """Save integrity check statistics to file"""
        if not self.stats_path:  # Skip if no stats path configured
            self.logger.debug("No stats path configured, skipping integrity stats save")
            return
            
        try:
            existing_stats = []
            if os.path.exists(self.stats_path):
                with open(self.stats_path, 'r') as f:
                    existing_stats = json.load(f)
            
            # Keep only last 10 stats
            existing_stats = existing_stats[-9:]
            existing_stats.append(stats)
            
            with open(self.stats_path, 'w') as f:
                json.dump(existing_stats, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving integrity stats: {e}")
    
    def save_performance_stats(self, stats):
        """Save performance statistics to file"""
        if not self.stats_path:  # Skip if no stats path configured
            self.logger.debug("No stats path configured, skipping performance stats save")
            return
            
        try:
            stats_file = self.stats_path.replace('.json', '_performance.json')
            existing_stats = []
            if os.path.exists(stats_file):
                with open(stats_file, 'r') as f:
                    existing_stats = json.load(f)
            
            # Keep only last 10 stats
            existing_stats = existing_stats[-9:]
            existing_stats.append(stats)
            
            with open(stats_file, 'w') as f:
                json.dump(existing_stats, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Error saving performance stats: {e}")
            
    def perform_maintenance(self):
        """Perform all maintenance tasks"""
        self.logger.info("Starting weekly database maintenance")
        
        try:
            # First perform integrity checks
            if not self.check_database_integrity():
                self.logger.error("Integrity checks failed, skipping further maintenance")
                return
            
            # Perform regular cleanup tasks
            self.cleanup_small_contests(10)
            self.cleanup_old_records(3)
            
            # Optimize performance
            self.optimize_performance()
            
            # Finally vacuum and reindex
            self.vacuum_database()
            self.reindex_database()
            
            self.logger.info("Weekly maintenance completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during maintenance: {e}")

def create_dummy_app():
    """Create a minimal Flask app for command line usage"""
    return Flask("maintenance")

def main():
    """CLI entry point for standalone operation"""
    parser = argparse.ArgumentParser(description='Database Maintenance Utility')
    parser.add_argument('--db', required=True,
                      help='Database file path')
    parser.add_argument('--log',
                      help='Log file path')
    parser.add_argument('--now', action='store_true',
                      help='Run maintenance immediately')
    
    args = parser.parse_args()

    try:
        maintenance = DatabaseMaintenance(args.db, args.log)
        
        if args.now:
            print("Starting immediate maintenance run...")
            maintenance.run_maintenance_now()
            print("Maintenance completed. Check logs for details.")
        else:
            parser.error('--now is required for command line operation')

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
