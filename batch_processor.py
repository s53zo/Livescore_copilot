#!/usr/bin/env python3
import queue
import threading
import time
import logging
import re # For sanitizing room names
from datetime import datetime

# Helper function (copied from web_interface.py or imported if refactored)
def _sanitize_room_name(name):
    """Remove potentially problematic characters for room names."""
    return re.sub(r'[^a-zA-Z0-9_-]', '_', name)

class BatchProcessor:
    def __init__(self, db_handler, socketio, batch_interval=60): # Added socketio parameter
        self.db_handler = db_handler
        self.socketio = socketio # Store socketio instance
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
                    # Parse data first
                    parsed_contest_data = self.db_handler.parse_xml_data(combined_xml)

                    if parsed_contest_data:
                        # Store data
                        self.db_handler.store_data(parsed_contest_data)

                        # --- Emit SocketIO notifications ---
                        updated_rooms = set()
                        for data_item in parsed_contest_data:
                            contest = data_item.get('contest')
                            callsign = data_item.get('callsign')
                            if contest and callsign:
                                safe_contest = _sanitize_room_name(contest)
                                safe_callsign = _sanitize_room_name(callsign)
                                room = f"{safe_contest}_{safe_callsign}"
                                updated_rooms.add(room)

                        if updated_rooms:
                            self.logger.info(f"Emitting notifications for rooms: {updated_rooms}")
                            for room in updated_rooms:
                                # Extract contest/callsign back from room name if needed for payload
                                # Or just send a simple notification
                                notification_payload = {'message': 'Scores updated'} # Simple notification
                                # Alternatively, extract from room: parts = room.split('_', 1); payload = {'contest': parts[0], 'callsign': parts[1]}
                                self.socketio.emit('score_updated_notification', notification_payload, room=room)
                        # ------------------------------------

                    batch_time = time.time() - batch_start
                    self.logger.info(f"Batch processed and notifications sent in {batch_time:.2f} seconds")

                except Exception as e:
                    self.logger.error(f"Error processing batch: {e}")
            
            elapsed = time.time() - start_time
            sleep_time = max(0, self.batch_interval - elapsed)
            time.sleep(sleep_time)
