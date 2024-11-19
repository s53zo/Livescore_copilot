class DatabaseMaintenance:
    """Database maintenance scheduler with integrated score cleanup"""
    def __init__(self, db_path, log_path=None):
        self.db_path = db_path
        self.setup_logging(log_path)
        self._maintenance_thread = None
        self._stop_flag = False
        self.stats_path = os.path.join(os.path.dirname(log_path), 'maintenance_stats.json') if log_path else None
        self.last_cleanup_time = datetime.now()
        self.last_maintenance_time = datetime.now()
        
    def cleanup_scores(self, minutes=90):
        """Cleanup old score records, keeping only the last 'minutes' of data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
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
                    
                    # Find IDs of old entries
                    cursor.execute("""
                        SELECT id FROM contest_scores
                        WHERE callsign = ? AND contest = ? AND timestamp < ?
                    """, (entry_callsign, entry_contest, cutoff_time.strftime('%Y-%m-%d %H:%M:%S')))
                    old_ids = [row[0] for row in cursor.fetchall()]
                    
                    if old_ids:
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
                        
                        deleted_count = len(old_ids)
                        total_deleted += deleted_count
                        self.logger.debug(
                            f"Cleaned up {deleted_count} old records for "
                            f"{entry_callsign} in {entry_contest}"
                        )
                
                conn.commit()
                self.logger.info(f"Score cleanup completed: {total_deleted} records removed")
                return total_deleted
                
        except Exception as e:
            self.logger.error(f"Error in score cleanup: {e}")
            self.logger.debug(traceback.format_exc())
            return 0

    def maintenance_worker(self):
        """Enhanced background worker that handles both regular maintenance and score cleanup"""
        while not self._stop_flag:
            try:
                now = datetime.now()
                
                # Check for score cleanup (every 15 minutes)
                if (now - self.last_cleanup_time).total_seconds() >= 120:  # 15 minutes = 900 seconds
                    self.logger.info("Starting scheduled score cleanup")
                    self.cleanup_scores(minutes=90)
                    self.last_cleanup_time = now
                
                # Check for weekly maintenance (Thursday 3 AM)
                if self.is_maintenance_time() and (now - self.last_maintenance_time).days >= 1:
                    self.logger.info("Starting scheduled weekly maintenance")
                    self.perform_maintenance()
                    self.last_maintenance_time = now
                
                # Sleep for 30 seconds between checks
                time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"Error in maintenance worker: {e}")
                self.logger.debug(traceback.format_exc())
                time.sleep(30)

    def perform_maintenance(self):
        """Perform all maintenance tasks including score cleanup"""
        self.logger.info("Starting weekly database maintenance")
        
        try:
            # First perform integrity checks
            if not self.check_database_integrity():
                self.logger.error("Integrity checks failed, skipping further maintenance")
                return
            
            # Perform score cleanup first
            self.cleanup_scores(90)
            
            # Setup/verify indexes
            self.setup_indexes()
            
            # Regular cleanup tasks
            self.cleanup_small_contests(10)
            self.cleanup_old_records(3)
            
            # Optimize performance
            self.optimize_performance()
            
            # Finally vacuum and reindex
            self.vacuum_database()
            self.reindex_database()
            
            # Analyze common queries
            for query in ["latest_scores", "band_breakdown", "contest_summary"]:
                self.explain_query(query)
            
            self.logger.info("Weekly maintenance completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during maintenance: {e}")
            self.logger.debug(traceback.format_exc())
