#!/usr/bin/env python3
import logging
import urllib.parse
from http.server import HTTPServer
from custom_handler import CustomHandler
from database_handler import ContestDatabaseHandler

class ContestServer:
    # Added mqtt_config parameter
    def __init__(self, host='127.0.0.1', port=8088, db_path='contest_data.db', debug=False, mqtt_config=None):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.debug = debug
        self.logger = self._setup_logging(debug)
        # Pass mqtt_config to ContestDatabaseHandler
        self.db_handler = ContestDatabaseHandler(db_path, mqtt_config=mqtt_config)
        
    def _setup_logging(self, debug):
        logger = logging.getLogger('ContestServer')
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
                
    def start(self):
        """Start the server"""
        try:
            server_address = (self.host, self.port)
            httpd = CustomServer(server_address, 
                               lambda *args, **kwargs: CustomHandler(*args, 
                                                                  debug_mode=self.debug, 
                                                                  **kwargs))
            httpd.db_handler = self.db_handler
            
            self.logger.info(f"Starting server on {self.host}:{self.port}")
            httpd.serve_forever()
            
        except Exception as e:
            self.logger.error(f"Error starting server: {e}")
            raise
        finally:
            self.cleanup()
            
    def cleanup(self):
        """Cleanup resources"""
        try:
            if hasattr(self, 'db_handler'):
                self.db_handler.cleanup()
                self.logger.info("Database handler cleaned up")
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

class CustomServer(HTTPServer):
    # Removed redundant db_handler creation and debug parameter here
    # The handler is passed down via the CustomHandler factory in ContestServer.start
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.db_handler = ContestDatabaseHandler(db_path, mqtt_config=mqtt_config) # Removed redundant creation
        # self.debug = debug # Removed redundant attribute

    # server_close cleanup is handled by ContestServer.cleanup calling db_handler.cleanup
    # No need for redundant cleanup here if db_handler is not owned by CustomServer
    # def server_close(self):
    #     if hasattr(self, 'db_handler'): # This db_handler wouldn't exist anymore
    #         self.db_handler.cleanup()
    #     super().server_close()
