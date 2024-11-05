#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for
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
            cursor = db.cursor()
            
            # Get contests
            logger.debug("Fetching contests")
            cursor.execute("""
                SELECT DISTINCT contest 
                FROM contest_scores 
                ORDER BY contest
            """)
            contests = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found contests: {contests}")
            
            # If contest is selected (either via POST or GET parameter)
            selected_contest = request.form.get('contest') or request.args.get('contest')
            logger.debug(f"Selected contest: {selected_contest}")
            
            callsigns = []
            countries = []
            cq_zones = []
            iaru_zones = []
            
            if selected_contest:
                # Get callsigns for this contest only
                logger.debug(f"Fetching callsigns for contest: {selected_contest}")
                cursor.execute("""
                    WITH LatestScores AS (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    )
                    SELECT cs.callsign
                    FROM contest_scores cs
                    JOIN LatestScores ls ON cs.callsign = ls.callsign 
                        AND cs.timestamp = ls.max_ts
                    WHERE cs.contest = ?
                    ORDER BY cs.callsign
                """, (selected_contest, selected_contest))
                callsigns = [row[0] for row in cursor.fetchall()]
                logger.debug(f"Found callsigns: {len(callsigns)}")
                
                # Get available DXCC countries for this contest
                cursor.execute("""
                    WITH LatestScores AS (
                        SELECT cs.id
                        FROM contest_scores cs
                        INNER JOIN (
                            SELECT callsign, MAX(timestamp) as max_ts
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        WHERE cs.contest = ?
                    )
                    SELECT DISTINCT qi.dxcc_country
                    FROM qth_info qi
                    JOIN LatestScores ls ON qi.contest_score_id = ls.id
                    WHERE qi.dxcc_country IS NOT NULL 
                    AND qi.dxcc_country != ''
                    ORDER BY qi.dxcc_country
                """, (selected_contest, selected_contest))
                countries = [row[0] for row in cursor.fetchall()]
                
                # Get available CQ zones for this contest
                cursor.execute("""
                    WITH LatestScores AS (
                        SELECT cs.id
                        FROM contest_scores cs
                        INNER JOIN (
                            SELECT callsign, MAX(timestamp) as max_ts
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        WHERE cs.contest = ?
                    )
                    SELECT DISTINCT qi.cq_zone
                    FROM qth_info qi
                    JOIN LatestScores ls ON qi.contest_score_id = ls.id
                    WHERE qi.cq_zone IS NOT NULL 
                    AND qi.cq_zone != ''
                    ORDER BY CAST(qi.cq_zone AS INTEGER)
                """, (selected_contest, selected_contest))
                cq_zones = [row[0] for row in cursor.fetchall()]
                
                # Get available IARU zones for this contest
                cursor.execute("""
                    WITH LatestScores AS (
                        SELECT cs.id
                        FROM contest_scores cs
                        INNER JOIN (
                            SELECT callsign, MAX(timestamp) as max_ts
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        WHERE cs.contest = ?
                    )
                    SELECT DISTINCT qi.iaru_zone
                    FROM qth_info qi
                    JOIN LatestScores ls ON qi.contest_score_id = ls.id
                    WHERE qi.iaru_zone IS NOT NULL 
                    AND qi.iaru_zone != ''
                    ORDER BY CAST(qi.iaru_zone AS INTEGER)
                """, (selected_contest, selected_contest))
                iaru_zones = [row[0] for row in cursor.fetchall()]
        
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
                success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                if success:
                    logger.info("Report generated successfully")
                    return redirect('/reports/live.html')
                else:
                    logger.error("Failed to generate report")
                    return render_template('error.html', error="Failed to generate report")
            else:
                logger.warning("No stations found")
                return render_template('error.html', error="No data found for the selected criteria")
        
        logger.debug("Rendering template with data")
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

# Add error handlers
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
