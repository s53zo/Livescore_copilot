#!/usr/bin/env python3
import logging
import urllib.parse
from http.server import HTTPServer
from custom_handler import CustomHandler
from database_handler import ContestDatabaseHandler

class ContestServer:
    def __init__(self, host='127.0.0.1', port=8088, db_path='contest_data.db', debug=False):
     with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")    self.host = host
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
            # Initialize database connection pool
            self.db_handler.setup_connection_pool()
            
            server_address = (self.host, self.port)
            httpd = CustomServer(server_address, 
                               lambda *args, **kwargs: CustomHandler(*args, 
                                                                  debug_mode=self.debug, 
                                                                  **kwargs))
            httpd.db_handler = self.db_handler
            
            self.logger.info(f"Starting server on {self.host}:{self.port}")
            httpd.serve_forever()
            
        except OSError as e:
            self.logger.critical(f"Port {self.port} unavailable: {e}")
            raise SystemExit(1) from e
        except DatabaseError as e:
            self.logger.critical(f"Database connection failed: {e}")
            raise SystemExit(1) from e
        except Exception as e:
            self.logger.critical(f"Critical server error: {e}", exc_info=self.debug)
            raise SystemExit(1) from e
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
        try:
            self.db_handler = ContestDatabaseHandler(db_path)
            self.db_handler.setup_connection_pool()
        except OperationalError as e:
            logging.critical(f"Failed to initialize database: {e}")
            raise SystemExit(1) from e
        self.debug = debug
                
    def server_close(self):
        try:
            if hasattr(self, 'db_handler') and self.db_handler:
                self.db_handler.cleanup()
                self.db_handler = None
        except Exception as e:
            logging.error(f"Error during server shutdown: {e}")
        finally:
            super().server_close()
