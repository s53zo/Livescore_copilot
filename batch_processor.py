#!/usr/bin/env python3
import queue
import threading
import time
import logging
from datetime import datetime

class BatchProcessor:
    def __init__(self, db_handler, batch_interval=30):
        self.db_handler = db_handler
        self.batch_interval = batch_interval
        self.queue = queue.Queue()
        self.is_running = False
        self.processing_thread = None
        self.batch_size = 0
        self.logger = logging.getLogger('BatchProcessor')
        self.last_processed_data = {}  # Track last processed data for each callsign
        self.change_callbacks = []  # List of callbacks to notify on changes
        
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
            
    def register_callback(self, callback):
        """Register a callback to be notified of changes"""
        if callback not in self.change_callbacks:
            self.change_callbacks.append(callback)
            self.logger.debug(f"Registered new callback. Total callbacks: {len(self.change_callbacks)}")
            
    def unregister_callback(self, callback):
        """Unregister a change callback"""
        if callback in self.change_callbacks:
            self.change_callbacks.remove(callback)
            self.logger.debug(f"Unregistered callback. Total callbacks: {len(self.change_callbacks)}")
    
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
        last_update_time = 0
        
        while self.is_running:
            current_time = time.time()
            
            # Only process if we've reached the batch interval
            if current_time - last_update_time >= self.batch_interval:
                start_time = time.time()
                batch = []
                
                # Collect all available items but limit to batch_interval
                try:
                    while not self.queue.empty() and len(batch) < 100:  # Limit batch size
                        batch.append(self.queue.get_nowait())
                        self.batch_size -= 1
                except queue.Empty:
                    pass
                
                # Ensure we wait the full interval even if we processed items
                last_update_time = current_time
                
                if batch:
                    try:
                        batch_start = time.time()
                        self.logger.info(f"Processing batch of {len(batch)} items")
                        
                        combined_xml = "\n".join(batch)
                        contest_data = self.db_handler.parse_xml_data(combined_xml)
                        
                        if contest_data:
                            # Filter for changed records only
                            changed_records = []
                            for record in contest_data:
                                callsign = record['callsign']
                                if callsign not in self.last_processed_data or \
                                   self.last_processed_data[callsign] != record:
                                    changed_records.append(record)
                                    self.last_processed_data[callsign] = record
                            
                            if changed_records:
                                self.db_handler.store_data(changed_records)
                                self.logger.info(f"Processed {len(changed_records)} changed records")
                                
                                # Store changed records for batch notification
                                self.pending_updates = changed_records
                                
                                # Update last processed time
                                last_update_time = time.time()
                        
                        batch_time = time.time() - batch_start
                        self.logger.info(f"Batch processed in {batch_time:.2f} seconds")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing batch: {e}")
                
                elapsed = time.time() - start_time
                sleep_time = max(0, self.batch_interval - elapsed)
                time.sleep(sleep_time)
                
                # Notify callbacks at the end of the batch interval
                if hasattr(self, 'pending_updates') and self.pending_updates:
                    for callback in self.change_callbacks:
                        try:
                            callback(self.pending_updates)
                        except Exception as e:
                            self.logger.error(f"Error in callback: {e}")
                    del self.pending_updates
            else:
                # Sleep briefly to prevent busy waiting
                time.sleep(0.1)

# Create a shared processor instance
from database_handler import ContestDatabaseHandler as DatabaseHandler
shared_processor = BatchProcessor(db_handler=DatabaseHandler(), batch_interval=30)
shared_processor.start()

# Export the BatchProcessor class and shared instance
__all__ = ['BatchProcessor', 'shared_processor']
