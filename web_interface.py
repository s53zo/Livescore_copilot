#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify
import sqlite3
import os
import logging
import sys
import traceback
from score_reporter import ScoreReporter
from datetime import datetime

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
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

def get_station_category(db, contest, callsign):
    """Get the category information for a specific station"""
    try:
        cursor = db.cursor()
        cursor.execute("""
            SELECT power, assisted
            FROM contest_scores
            WHERE contest = ? AND callsign = ?
            ORDER BY timestamp DESC
            LIMIT 1
        """, (contest, callsign))
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Error getting station category: {str(e)}")
        return None

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    logger.debug(f"Request received: {request.method}")
    logger.debug(f"Request form data: {request.form}")
    logger.debug(f"Request headers: {request.headers}")

    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Get contests with the count of unique active stations (callsigns)
            logger.debug("Fetching contests with active station counts")
            cursor.execute("""
                SELECT contest, COUNT(DISTINCT callsign) AS active_stations
                FROM contest_scores
                GROUP BY contest
                ORDER BY contest
            """)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            logger.debug(f"Found contests with station counts: {contests}")
            
            # Get contest and callsign from form or query parameters
            selected_contest = request.form.get('contest') or request.args.get('contest')
            selected_callsign = request.form.get('callsign') or request.args.get('callsign')
            
            logger.debug(f"Selected contest: {selected_contest}")
            logger.debug(f"Selected callsign: {selected_callsign}")
            
            callsigns = []
            countries = []
            cq_zones = []
            iaru_zones = []
            station_category = None
            
            if selected_contest:
                # Fetch callsigns with QSO count for the selected contest
                logger.debug(f"Fetching callsigns with QSO count for contest: {selected_contest}")
                cursor.execute("""
                    SELECT callsign, COUNT(*) AS qso_count
                    FROM contest_scores
                    WHERE contest = ?
                    GROUP BY callsign
                    ORDER BY callsign
                """, (selected_contest,))
                callsigns = [{"name": row[0], "qso_count": row[1]} for row in cursor.fetchall()]
                
                # If callsign is selected, get its category
                if selected_callsign:
                    station_category = get_station_category(db, selected_contest, selected_callsign)
                    logger.debug(f"Station category for {selected_callsign}: {station_category}")
                
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
            category_filter = request.form.get('category_filter', 'same')  # 'same' or 'all'
            
            # Select only the last non-empty filter_value
            filter_values = request.form.getlist('filter_value')
            filter_value = next((fv for fv in reversed(filter_values) if fv), None)

            # Validate filter inputs
            if filter_type and not filter_value:
                return render_template('error.html', 
                                     error="Filter type selected but no value provided")
        
            logger.info(f"Processing report request: callsign={callsign}, contest={contest}, "
                       f"filter_type={filter_type}, filter_value={filter_value}, "
                       f"category_filter={category_filter}")
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest, filter_type, filter_value, 
                                                 category_filter=category_filter)
            
            if stations:
                success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR, 
                                              filter_type=filter_type, filter_value=filter_value)
                if success:
                    query_params = [f"callsign={callsign}", f"contest={contest}"]
                    if filter_type and filter_value:
                        query_params.extend([f"filter_type={filter_type}", 
                                          f"filter_value={filter_value}"])
                    if category_filter:
                        query_params.append(f"category_filter={category_filter}")
                    
                    return redirect(f'/reports/live.html?{"&".join(query_params)}')
                else:
                    return render_template('error.html', error="Failed to generate report")
            else:
                return render_template('error.html', error="No data found for the selected criteria")
        
        logger.debug("Rendering template")
        return render_template('select_form.html', 
                             contests=contests,
                             selected_contest=selected_contest,
                             selected_callsign=selected_callsign,
                             callsigns=callsigns,
                             countries=countries,
                             cq_zones=cq_zones,
                             iaru_zones=iaru_zones,
                             station_category=station_category)
    
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
        category_filter = request.args.get('category_filter', 'same')

        if not (callsign and contest):
            return render_template('error.html', error="Missing required parameters")

        logger.info(f"Refreshing report for: callsign={callsign}, contest={contest}, "
                   f"filter_type={filter_type}, filter_value={filter_value}, "
                   f"category_filter={category_filter}")

        reporter = ScoreReporter(Config.DB_PATH)
        stations = reporter.get_station_details(callsign, contest, filter_type, filter_value,
                                             category_filter=category_filter)

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
    logger.info("Starting development server")
    app.run(host='127.0.0.1', port=8089)
else:
    # When running under gunicorn
    logger.info("Starting under gunicorn")

