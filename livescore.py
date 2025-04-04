#!/usr/bin/env python3
# Apply gevent monkey patching early
import gevent.monkey
gevent.monkey.patch_all()

import argparse
import logging
import sys
# Import Flask and SocketIO for factory
from flask import Flask
from flask_socketio import SocketIO
# Import the registration function and logger from web_interface
from web_interface import register_routes_and_handlers, logger as web_logger # Use web_logger to avoid name clash
# Import ContestDatabaseHandler directly
from database_handler import ContestDatabaseHandler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from maintenance_task import perform_maintenance
import os
import fcntl  # For file locking
import errno # For error codes
import argparse # Keep for main()

# Apply gevent monkey patching early
import gevent.monkey
gevent.monkey.patch_all()


def setup_logging(debug_mode, log_file):
    """Setup logging configuration - Returns the root logger"""
    log_level = logging.DEBUG if debug_mode else logging.INFO
    
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger

# Define lock file path (use /tmp or another suitable directory)
MAINTENANCE_LOCK_FILE = '/tmp/livescore_maintenance.lock'

def run_maintenance(db_path, logger):
    """Run maintenance tasks with logging and file locking."""
    lock_file_handle = None
    try:
        # Attempt to acquire the lock
        logger.debug(f"Attempting to acquire maintenance lock: {MAINTENANCE_LOCK_FILE}")
        lock_file_handle = open(MAINTENANCE_LOCK_FILE, 'w')
        fcntl.lockf(lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger.info("Successfully acquired maintenance lock.")

        # --- Maintenance Logic Starts Here ---
        logger.info("Starting scheduled maintenance tasks...")

        # Create maintenance directories if they don't exist
        backup_dir = "./backups"
        reports_dir = "./reports"
        archive_dir = "./archive"
        for directory in [backup_dir, reports_dir, archive_dir]:
            os.makedirs(directory, exist_ok=True)

        # Run maintenance with dry_run=False
        perform_maintenance(db_path, dry_run=False)
        logger.info("Scheduled maintenance completed successfully.")
        # --- Maintenance Logic Ends Here ---

    except (IOError, BlockingIOError) as e:
        # Check if it's specifically a locking error
        if e.errno == errno.EACCES or e.errno == errno.EAGAIN:
            logger.debug("Maintenance lock already held by another process. Skipping run.")
        else:
            # Log other IOErrors
            logger.error(f"IOError during maintenance lock/run: {e}")
            logger.exception("Maintenance task IO error details:")
    except Exception as e:
        # Log any other exceptions during maintenance execution
        logger.error(f"Error during scheduled maintenance execution: {e}")
        logger.exception("Maintenance task execution error details:")
    finally:
        # Ensure the lock is always released if the handle was acquired
        if lock_file_handle:
            try:
                fcntl.lockf(lock_file_handle, fcntl.LOCK_UN)
                lock_file_handle.close()
                logger.debug("Released maintenance lock.")
                # Optionally remove the lock file after release, though not strictly necessary
                # try:
                #     os.remove(MAINTENANCE_LOCK_FILE)
                # except OSError:
                #     pass
            except Exception as unlock_e:
                logger.error(f"Error releasing maintenance lock: {unlock_e}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Contest Data Server')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='Enable debug mode')
    # Host/Port now apply to the Flask/SocketIO server
    parser.add_argument('--host', default='127.0.0.1',
                      help='Host for Flask/SocketIO server (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8089, # Default changed to match old Flask dev port
                      help='Port for Flask/SocketIO server (default: 8089)')
    # Log file might need renaming or consolidation if desired
    parser.add_argument('--log-file', default='livescore_app.log',
                      help='Log file path (default: livescore_app.log)')
    parser.add_argument('--db-file', default='/opt/livescore/contest_data.db', # Match default in Config
                      help='Database file path (default: contest_data.db)')
    parser.add_argument('--maintenance-hour', type=int, default=2,
                      help='Hour to run maintenance (24-hour format, default: 2)')
    parser.add_argument('--maintenance-minute', type=int, default=0,
                      help='Minute to run maintenance (default: 0)')
    return parser.parse_args()

# +++ Application Factory Function +++
def create_app(db_path='/opt/livescore/contest_data.db', debug=False):
    """Application Factory"""
    web_logger.info("Creating Flask application instance...")
    # Note: Using web_logger assuming it's configured in web_interface.py
    # If logging needs to be configured here, adjust accordingly.
    app = Flask(__name__)
    app.config['DEBUG'] = debug
    # Add any other necessary app configurations here (e.g., SECRET_KEY if using sessions)

    web_logger.info("Initializing Flask-SocketIO...")
    # Initialize SocketIO here, associated with the app
    socketio = SocketIO(app, async_mode='gevent', cors_allowed_origins="*")
    web_logger.info("Flask-SocketIO initialized successfully")

    # Initialize Database Handler, passing the created socketio instance
    # This also starts the BatchProcessor thread internally
    web_logger.info("Initializing Database Handler and Batch Processor...")
    db_handler = ContestDatabaseHandler(db_path=db_path, socketio=socketio)

    # Store the handler in app config
    app.config['DB_HANDLER'] = db_handler
    web_logger.info("Database Handler instance stored in app.config['DB_HANDLER']")

    # Register Blueprints or routes defined in web_interface.py
    register_routes_and_handlers(app, socketio)
    web_logger.info("Registered routes and SocketIO handlers.")

    # Return the configured app and socketio (socketio needed for running)
    return app, socketio
# --- End of Application Factory ---


# --- Main Execution Logic (for direct run `python livescore.py`) ---
def main():
    # Parse command line arguments
    args = parse_arguments()

    # Setup logging for direct execution (can use its own logger)
    # Using a separate logger instance for the main script execution part
    main_logger = setup_logging(args.debug, args.log_file)

    # Log startup information using main_logger
    main_logger.info("Livescore Application starting up directly with configuration:")
    main_logger.info(f"Flask/SocketIO Host: {args.host}")
    main_logger.info(f"Flask/SocketIO Port: {args.port}")
    main_logger.info(f"Debug Mode: {'ON' if args.debug else 'OFF'}")
    main_logger.info(f"Log File: {args.log_file}")
    main_logger.info(f"Database File: {args.db_file}")
    main_logger.info(f"Maintenance Time: {args.maintenance_hour:02d}:{args.maintenance_minute:02d}")

    # Create the app using the factory
    # Pass relevant args like db_file and debug status
    app, socketio = create_app(db_path=args.db_file, debug=args.debug)

    # Initialize scheduler (can also be part of create_app if preferred)
    scheduler = BackgroundScheduler()
    # Add maintenance job
    trigger = CronTrigger(
        hour=args.maintenance_hour,
        minute=args.maintenance_minute
    )
    scheduler.add_job(
        run_maintenance,
        trigger=trigger,
        args=[args.db_file, main_logger], # Pass main_logger here
        id='maintenance_job',
        name='Database Maintenance',
        misfire_grace_time=3600  # Allow job to run up to 1 hour late
    )
    # Start the scheduler
    scheduler.start()
    main_logger.info(f"Scheduled maintenance job for {args.maintenance_hour:02d}:{args.maintenance_minute:02d}")

    # db_handler is now initialized inside create_app and stored in app.config

    main_logger.info("Starting Flask-SocketIO development server...")
    try:
        # Run using the socketio instance returned by the factory
        # Use host/port from arguments
        # debug=args.debug enables Flask debug mode (auto-reload, etc.)
        # Use use_reloader=False if APScheduler conflicts with Flask reloader
        socketio.run(app, host=args.host, port=args.port, debug=args.debug, use_reloader=False)

    except KeyboardInterrupt:
        main_logger.info("Received shutdown signal (KeyboardInterrupt)")
    except SystemExit:
        main_logger.info("Received shutdown signal (SystemExit)")
    except Exception as e:
        main_logger.error(f"Error starting Flask-SocketIO server: {e}")
        main_logger.exception("Server error details:")
        # No need to raise again, just log and exit finally block
    finally:
        # Cleanup
        main_logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=False) # Don't wait for jobs to finish on shutdown
        # Cleanup db_handler (access it from app.config)
        # Need to check if 'app' exists in case create_app failed
        if 'app' in locals():
            db_handler = app.config.get('DB_HANDLER')
            if db_handler:
                 main_logger.info("Cleaning up Database Handler (stops batch processor)...")
                 db_handler.cleanup()
        main_logger.info("Application shutdown complete.")

if __name__ == "__main__":
    main()
