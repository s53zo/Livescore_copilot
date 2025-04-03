#!/usr/bin/env python3
# Apply gevent monkey patching early
import gevent.monkey
gevent.monkey.patch_all()

import argparse
import logging
import sys
# Import the app and socketio instance from web_interface
from web_interface import app, socketio
# Import ContestDatabaseHandler directly
from database_handler import ContestDatabaseHandler
# from contest_server import ContestServer # No longer needed
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from maintenance_task import perform_maintenance
import os
import fcntl  # For file locking
import errno # For error codes

def setup_logging(debug_mode, log_file):
    """Setup logging configuration"""
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

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging (might need adjustment if Flask/SocketIO logging is configured differently)
    logger = setup_logging(args.debug, args.log_file)

    # Log startup information
    logging.info("Livescore Application starting up with configuration:")
    logging.info(f"Flask/SocketIO Host: {args.host}")
    logging.info(f"Flask/SocketIO Port: {args.port}")
    logging.info(f"Debug Mode: {'ON' if args.debug else 'OFF'}")
    logging.info(f"Log File: {args.log_file}")
    logging.info(f"Database File: {args.db_file}")
    logging.info(f"Maintenance Time: {args.maintenance_hour:02d}:{args.maintenance_minute:02d}")
    
    # Initialize scheduler
    scheduler = BackgroundScheduler()
    
    # Add maintenance job
    trigger = CronTrigger(
        hour=args.maintenance_hour,
        minute=args.maintenance_minute
    )
    
    scheduler.add_job(
        run_maintenance,
        trigger=trigger,
        args=[args.db_file, logger],
        id='maintenance_job',
        name='Database Maintenance',
        misfire_grace_time=3600  # Allow job to run up to 1 hour late
    )
    
    # Start the scheduler
    scheduler.start()
    logger.info(f"Scheduled maintenance job for {args.maintenance_hour:02d}:{args.maintenance_minute:02d}")

    # Initialize ContestDatabaseHandler directly, passing socketio
    # This also starts the BatchProcessor thread internally
    logger.info("Initializing Database Handler and Batch Processor...")
    db_handler = ContestDatabaseHandler(db_path=args.db_file, socketio=socketio)

    # Make db_handler accessible to Flask routes
    app.config['DB_HANDLER'] = db_handler
    logger.info("Database Handler instance stored in app.config['DB_HANDLER']")

    # Remove old ContestServer initialization
    # contest_server_instance = ContestServer(...)

    logger.info("Starting Flask-SocketIO server...")
    try:
        # Run the Flask app with SocketIO
        # Use host/port from arguments
        # debug=args.debug enables Flask debug mode (auto-reload, etc.)
        # Use use_reloader=False if APScheduler conflicts with Flask reloader
        socketio.run(app, host=args.host, port=args.port, debug=args.debug, use_reloader=False)

    except KeyboardInterrupt:
        logger.info("Received shutdown signal (KeyboardInterrupt)")
    except SystemExit:
        logger.info("Received shutdown signal (SystemExit)")
    except Exception as e:
        logger.error(f"Error starting Flask-SocketIO server: {e}")
        logger.exception("Server error details:")
        # No need to raise again, just log and exit finally block
    finally:
        # Cleanup
        logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=False) # Don't wait for jobs to finish on shutdown
        # Cleanup db_handler (which stops the batch processor)
        if 'db_handler' in locals():
             logger.info("Cleaning up Database Handler (stops batch processor)...")
             db_handler.cleanup()
        # Remove old ContestServer cleanup
        # if 'contest_server_instance' in locals():
        #     contest_server_instance.cleanup()
        logger.info("Application shutdown complete.")

if __name__ == "__main__":
    main()
