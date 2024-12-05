#!/usr/bin/env python3
import queue
import threading
import time
import logging
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
    
    def _process_batch_loop(self):
        """Main processing loop - runs every batch_interval seconds"""
        while self.is_running:
            start_time = time.time()
            batch = []
            
            try:
                while True:
                    batch.append(self.queue.get_nowait())
                    self.batch_size -= 1
            except queue.Empty:
                pass
            
            if batch:
                try:
                    batch_start = time.time()
                    self.logger.info(f"Processing batch of {len(batch)} items")
                    
                    combined_xml = "\n".join(batch)
                    contest_data = self.db_handler.parse_xml_data(combined_xml)
                    if contest_data:
                        self.db_handler.store_data(contest_data)
                    
                    batch_time = time.time() - batch_start
                    self.logger.info(f"Batch processed in {batch_time:.2f} seconds")
                    
                except Exception as e:
                    self.logger.error(f"Error processing batch: {e}")
            
            elapsed = time.time() - start_time
            sleep_time = max(0, self.batch_interval - elapsed)
            time.sleep(sleep_time)
