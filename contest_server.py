#!/usr/bin/env python3
import logging
import urllib.parse
from http.server import HTTPServer
from custom_handler import CustomHandler
from database_handler import ContestDatabaseHandler

class ContestServer:
    def __init__(self, host='127.0.0.1', port=8088, db_path='contest_data.db', debug=False):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.debug = debug
        self.logger = self._setup_logging(debug)
        self.db_handler = ContestDatabaseHandler(db_path)
        
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
    def __init__(self, *args, db_path='contest_data.db', debug=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.db_handler = ContestDatabaseHandler(db_path)
        self.debug = debug
                
    def server_close(self):
        if hasattr(self, 'db_handler'):
            self.db_handler.cleanup()
        super().server_close()
