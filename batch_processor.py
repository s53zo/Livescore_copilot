#!/usr/bin/env python3
import queue
import threading
import time
import logging
import sqlite3
import traceback
from datetime import datetime

class BatchProcessor:
    def __init__(self, db_handler, batch_interval=60):
        self.db_handler = db_handler
        self.batch_interval = batch_interval
        self.queue = queue.Queue()
        self.is_running = False
        self.processing_thread = None
        self.batch_size = 0
        self.logger = logging.getLogger('BatchProcessor')
        
    def start(self):
        """Start the batch processing thread"""
        if not self.is_running:
            self.is_running = True
            self.processing_thread = threading.Thread(target=self._process_batch_loop)
            self.processing_thread.daemon = True
            self.processing_thread.start()
            self.logger.info("Batch processor started")
    
    def stop(self):
        """Stop the batch processing thread"""
        self.is_running = False
        if self.processing_thread:
            self.processing_thread.join()
            self.logger.info("Batch processor stopped")
    
    def add_to_batch(self, xml_data):
        """Add XML data to processing queue"""
        self.queue.put(xml_data)
        self.batch_size += 1
        self.logger.debug(f"Added to batch. Current size: {self.batch_size}")

    # In batch_processor.py, add:
    def pause_processing(self):
        """Pause batch processing temporarily"""
        self.paused = True
        
    def resume_processing(self):
        """Resume batch processing"""
        self.paused = False
    
    def _process_batch_loop(self):
        """Main processing loop - runs every batch_interval seconds"""
        while self.is_running:
            start_time = time.time()
            batch = []
            
            try:
                # Collect items from queue
                while True:
                    try:
                        batch.append(self.queue.get_nowait())
                        self.batch_size -= 1
                    except queue.Empty:
                        break
                
                if batch:
                    try:
                        batch_start = time.time()
                        self.logger.info(f"Processing batch of {len(batch)} items")
                        
                        # Try to connect with timeout and process the batch
                        try:
                            with sqlite3.connect(self.db_path, timeout=30.0) as conn:
                                conn.execute("PRAGMA busy_timeout = 30000")
                                combined_xml = "\n".join(batch)
                                contest_data = self.db_handler.parse_xml_data(combined_xml)
                                if contest_data:
                                    self.db_handler.store_data(contest_data)
                                
                                batch_time = time.time() - batch_start
                                self.logger.info(f"Batch processed in {batch_time:.2f} seconds")
                                
                        except sqlite3.OperationalError as e:
                            if "database is locked" in str(e):
                                self.logger.warning("Database locked, waiting before retry...")
                                time.sleep(1)  # Wait before retry
                                # Put items back in queue
                                for item in batch:
                                    self.queue.put(item)
                                    self.batch_size += 1
                            else:
                                raise
                                
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
                        self.logger.error(traceback.format_exc())
                        # Put items back in queue on error
                        for item in batch:
                            self.queue.put(item)
                            self.batch_size += 1
                
                # Calculate and handle sleep time
                elapsed = time.time() - start_time
                sleep_time = max(0, self.batch_interval - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
            except Exception as e:
                self.logger.error(f"Error in batch processing loop: {e}")
                self.logger.error(traceback.format_exc())
                time.sleep(self.batch_interval)  # Wait before retry
