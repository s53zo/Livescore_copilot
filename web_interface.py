#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import os
import logging
import sys
import traceback
from score_reporter import ScoreReporter
from datetime import datetime

app = Flask(__name__)

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('/opt/livescore/logs/debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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
            cursor.execute("SELECT DISTINCT contest FROM contest_scores ORDER BY contest")
            contests = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found contests: {contests}")
            
            # Get callsigns
            logger.debug("Fetching callsigns")
            cursor.execute("SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign")
            callsigns = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found callsigns: {callsigns}")
            
            # Get available DXCC countries
            logger.debug("Fetching DXCC countries")
            cursor.execute("""
                SELECT DISTINCT dxcc_country 
                FROM qth_info 
                WHERE dxcc_country IS NOT NULL AND dxcc_country != ''
                ORDER BY dxcc_country
            """)
            countries = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found countries: {countries}")
            
            # Get available CQ zones
            logger.debug("Fetching CQ zones")
            cursor.execute("""
                SELECT DISTINCT cq_zone 
                FROM qth_info 
                WHERE cq_zone IS NOT NULL AND cq_zone != ''
                ORDER BY cq_zone
            """)
            cq_zones = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found CQ zones: {cq_zones}")
            
            # Get available IARU zones
            logger.debug("Fetching IARU zones")
            cursor.execute("""
                SELECT DISTINCT iaru_zone 
                FROM qth_info 
                WHERE iaru_zone IS NOT NULL AND iaru_zone != ''
                ORDER BY iaru_zone
            """)
            iaru_zones = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found IARU zones: {iaru_zones}")
        
        if request.method == 'POST':
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            filter_type = request.form.get('filter_type')
            filter_value = request.form.get('filter_value')
            
            logger.info(f"POST request with callsign={callsign}, contest={contest}, "
                       f"filter_type={filter_type}, filter_value={filter_value}")
            
            # Create reporter instance
            logger.debug("Creating ScoreReporter instance")
            reporter = ScoreReporter(Config.DB_PATH)
            
            # Get station details with filters
            logger.debug("Getting station details")
            stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)
            logger.debug(f"Station details result: {stations}")
            
            if stations:
                logger.debug("Generating HTML report")
                # Debug directory existence and permissions
                logger.debug(f"Output directory exists: {os.path.exists(Config.OUTPUT_DIR)}")
                logger.debug(f"Output directory permissions: {oct(os.stat(Config.OUTPUT_DIR).st_mode)[-3:]}")
                
                success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                logger.debug(f"HTML generation result: {success}")
                
                if success:
                    logger.info("Redirecting to report")
                    return redirect('/reports/live.html')
                else:
                    logger.error("Failed to generate report")
                    return render_template('error.html', error="Failed to generate report")
            else:
                logger.warning("No stations found")
                return render_template('error.html', error="No data found for the selected criteria")
        
        logger.debug("Rendering template")
        return render_template('select_form.html', 
                             contests=contests, 
                             callsigns=callsigns,
                             countries=countries,
                             cq_zones=cq_zones,
                             iaru_zones=iaru_zones)
    
    except Exception as e:
        logger.error("Exception occurred:")
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

if __name__ == '__main__':
    logger.info("Starting application")
    app.run(host='127.0.0.1', port=8089)
