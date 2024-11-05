#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, abort
import sqlite3
import os
import logging
import sys
import traceback
from score_reporter import ScoreReporter
from datetime import datetime

# Set up detailed logging before anything else
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG level
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('/opt/livescore/logs/debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Log startup
logger.info("Starting web interface application")

try:
    app = Flask(__name__)
    logger.info("Flask app created successfully")
except Exception as e:
    logger.error(f"Failed to create Flask app: {str(e)}")
    logger.error(traceback.format_exc())
    raise

class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'

def get_db():
    """Database connection with logging"""
    logger.debug("Attempting database connection")
    try:
        conn = sqlite3.connect(Config.DB_PATH)
        logger.debug("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    logger.debug(f"Request received: {request.method}")
    logger.debug(f"Request form data: {request.form}")
    logger.debug(f"Request headers: {request.headers}")

    try:
        with get_db() as db:
            # ... (keep existing database queries) ...

        if request.method == 'POST' and request.form.get('callsign'):
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            filter_type = request.form.get('filter_type')
            filter_value = request.form.get('filter_value')
            
            logger.info(f"Processing report request: callsign={callsign}, contest={contest}, "
                       f"filter_type={filter_type}, filter_value={filter_value}")
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)
            
            if stations:
                success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR, 
                                              filter_type=filter_type, filter_value=filter_value)
                if success:
                    # Build query parameters
                    query_params = f"?callsign={callsign}&contest={contest}"
                    if filter_type and filter_value:
                        query_params += f"&filter_type={filter_type}&filter_value={filter_value}"
                    
                    # Redirect to the report
                    return redirect(f'/reports/live.html{query_params}')
                else:
                    return render_template('error.html', error="Failed to generate report")
            else:
                return render_template('error.html', error="No data found for the selected criteria")
        
        logger.debug("Rendering template")
        return render_template('select_form.html', 
                             contests=contests,
                             selected_contest=selected_contest,
                             callsigns=callsigns,
                             countries=countries,
                             cq_zones=cq_zones,
                             iaru_zones=iaru_zones)
    
    except Exception as e:
        logger.error("Exception in index route:")
        logger.error(traceback.format_exc())
        return render_template('error.html', error=f"Error: {str(e)}")

@app.route('/reports/live.html')
def live_report():
    try:
        # Get parameters from query string
        callsign = request.args.get('callsign')
        contest = request.args.get('contest')
        filter_type = request.args.get('filter_type')
        filter_value = request.args.get('filter_value')

        if not (callsign and contest):
            return render_template('error.html', error="Missing required parameters")

        logger.info(f"Refreshing report for: callsign={callsign}, contest={contest}, "
                   f"filter_type={filter_type}, filter_value={filter_value}")

        reporter = ScoreReporter(Config.DB_PATH)
        stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)

        if stations:
            success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR, 
                                          filter_type=filter_type, filter_value=filter_value)
            if success:
                return send_from_directory(Config.OUTPUT_DIR, 'live.html')
            else:
                return render_template('error.html', error="Failed to generate report")
        else:
            return render_template('error.html', error="No data found for the selected criteria")

    except Exception as e:
        logger.error("Exception in live_report:")
        logger.error(traceback.format_exc())
        return render_template('error.html', error=f"Error: {str(e)}")

# Add static files route if not already present
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

# Add error handlers if not already present
@app.errorhandler(404)
def not_found_error(error):
    logger.error(f"404 error: {error}")
    return render_template('error.html', error="Page not found"), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"500 error: {error}")
    logger.error(traceback.format_exc())
    return render_template('error.html', error="Internal server error"), 500

# Only run the app if this file is run directly
if __name__ == '__main__':
    logger.info("Starting development server")
    app.run(host='127.0.0.1', port=8089)
else:
    # When running under gunicorn
    logger.info("Starting under gunicorn")

# Add gunicorn error handlers
def on_starting(server):
    logger.info("Gunicorn starting up")

def on_reload(server):
    logger.info("Gunicorn reloading")

def when_ready(server):
    logger.info("Gunicorn ready")

def on_exit(server):
    logger.info("Gunicorn shutting down")
