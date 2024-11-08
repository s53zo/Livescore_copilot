#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify, make_response
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

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    logger.debug(f"Request received: {request.method}")
    
    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Get contests with station counts
            cursor.execute("""
                WITH latest_scores AS (
                    SELECT callsign, contest, MAX(timestamp) as max_ts
                    FROM contest_scores
                    GROUP BY callsign, contest
                )
                SELECT 
                    cs.contest, 
                    COUNT(DISTINCT cs.callsign) as active_stations
                FROM contest_scores cs
                INNER JOIN latest_scores ls 
                    ON cs.callsign = ls.callsign 
                    AND cs.contest = ls.contest 
                    AND cs.timestamp = ls.max_ts
                GROUP BY cs.contest
                ORDER BY cs.contest
            """)
            
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            logger.debug(f"Found {len(contests)} contests")
            
            # Get contest and callsign from form or query parameters
            selected_contest = request.form.get('contest') or request.args.get('contest')
            selected_callsign = request.form.get('callsign') or request.args.get('callsign')
            
            if selected_contest:
                logger.debug(f"Selected contest: {selected_contest}")
            
            return render_template('select_form.html', 
                                 contests=contests,
                                 selected_contest=selected_contest,
                                 selected_callsign=selected_callsign,
                                 callsigns=[])  # Empty initial callsigns list
        
    except Exception as e:
        logger.error("Exception in index route:")
        logger.error(traceback.format_exc())
        return render_template('error.html', error=f"Error: {str(e)}")

@app.route('/reports/live.html')
def live_report():
    try:
        # Get parameters from URL
        callsign = request.args.get('callsign')
        contest = request.args.get('contest')
        filter_type = request.args.get('filter_type', 'none')
        filter_value = request.args.get('filter_value', 'none')

        if not (callsign and contest):
            return render_template('error.html', error="Missing required parameters")

        logger.info(f"Generating report for: contest={contest}, callsign={callsign}, "
                   f"filter_type={filter_type}, filter_value={filter_value}")

        # Create reporter instance
        reporter = ScoreReporter(Config.DB_PATH)

        # Verify contest and callsign exist in database
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM contest_scores 
                WHERE contest = ? AND callsign = ?
            """, (contest, callsign))
            if cursor.fetchone()[0] == 0:
                return render_template('error.html', 
                    error=f"No data found for {callsign} in {contest}")

        # Get station data with filters
        stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)

        if stations:
            # Generate HTML content directly
            template_path = os.path.join(os.path.dirname(__file__), 'templates', 'score_template.html')
            with open(template_path, 'r') as f:
                template = f.read()

            html_content = reporter.generate_html_content(template, callsign, contest, stations)
            
            # Return response with appropriate headers
            response = make_response(html_content)
            response.headers['Content-Type'] = 'text/html; charset=utf-8'
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
            
            logger.info(f"Successfully generated report for {callsign} in {contest}")
            return response
        else:
            logger.error(f"No station data found for {callsign} in {contest}")
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

@app.route('/livescore-pilot/api/contests')
def get_contests():
    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute("""
                SELECT contest, COUNT(DISTINCT callsign) AS active_stations
                FROM contest_scores
                GROUP BY contest
                ORDER BY contest
            """)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            return jsonify(contests)
    except Exception as e:
        logger.error(f"Error fetching contests: {str(e)}")
        return jsonify({"error": str(e)}), 500

def get_continent_from_dxcc(dxcc_country):
    """Get continent based on DXCC country from cty.plist"""
    try:
        import plistlib
        with open('/opt/livescore/cty.plist', 'rb') as fp:
            cty_data = plistlib.load(fp)
            
        # Search through the plist data for matching country
        for prefix_data in cty_data.values():
            if isinstance(prefix_data, dict):
                if prefix_data.get('Country') == dxcc_country:
                    return prefix_data.get('Continent', 'Unknown')
        return 'Unknown'
    except Exception as e:
        logger.error(f"Error reading cty.plist: {e}")
        return 'Unknown'

@app.route('/livescore-pilot/api/callsigns')
def get_callsigns():
    """Get callsigns for a contest with their QSO counts and continents"""
    contest = request.args.get('contest')
    if not contest:
        return jsonify({"error": "Contest parameter required"}), 400

    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Get the latest data for each callsign in the contest
            cursor.execute("""
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.qsos
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    ) latest 
                    ON cs.callsign = latest.callsign 
                    AND cs.timestamp = latest.max_ts
                    WHERE cs.contest = ?
                )
                SELECT 
                    ls.callsign,
                    ls.qsos,
                    qi.dxcc_country
                FROM latest_scores ls
                LEFT JOIN qth_info qi ON qi.contest_score_id = ls.id
                ORDER BY ls.callsign
            """, (contest, contest))

            results = cursor.fetchall()
            callsigns = []

            # Process each result and add continent information
            for row in results:
                callsign, qso_count, dxcc_country = row
                continent = get_continent_from_dxcc(dxcc_country) if dxcc_country else 'Unknown'
                
                callsigns.append({
                    "name": callsign,
                    "qso_count": qso_count or 0,  # Convert None to 0
                    "continent": continent
                })

            logger.debug(f"Returning {len(callsigns)} callsigns for contest {contest}")
            return jsonify(callsigns)

    except Exception as e:
        logger.error(f"Error in get_callsigns: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/livescore-pilot/api/filters')
def get_filters():
    contest = request.args.get('contest')
    callsign = request.args.get('callsign')
    
    if not contest or not callsign:
        return jsonify({"error": "Contest and callsign parameters required"}), 400

    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute("""
                SELECT qi.dxcc_country, qi.cq_zone, qi.iaru_zone, 
                       qi.arrl_section, qi.state_province
                FROM contest_scores cs
                JOIN qth_info qi ON qi.contest_score_id = cs.id
                WHERE cs.contest = ? AND cs.callsign = ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            """, (contest, callsign))
            
            row = cursor.fetchone()
            if not row:
                return jsonify([])

            filters = []
            filter_map = {
                'DXCC': row[0],
                'CQ Zone': row[1],
                'IARU Zone': row[2],
                'ARRL Section': row[3],
                'State/Province': row[4]
            }

            for filter_type, value in filter_map.items():
                if value:  # Only include non-empty values
                    filters.append({
                        "type": filter_type,
                        "value": value
                    })

            return jsonify(filters)
    except Exception as e:
        logger.error(f"Error fetching filters: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting development server")
    app.run(host='127.0.0.1', port=8089)
else:
    # When running under gunicorn
    logger.info("Starting under gunicorn")
