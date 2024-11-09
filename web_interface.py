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
                SELECT contest, COUNT(DISTINCT callsign) AS active_stations
                FROM contest_scores
                GROUP BY contest
                ORDER BY contest
            """)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            # Get contest and callsign from form or query parameters
            selected_contest = request.form.get('contest') or request.args.get('contest')
            selected_callsign = request.form.get('callsign') or request.args.get('callsign')
            
            callsigns = []
            
            if selected_contest:
                # Fetch callsigns with QSO count for the selected contest
                cursor.execute("""
                    SELECT cs.callsign, cs.qsos AS qso_count
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.timestamp = latest.max_ts
                    WHERE cs.contest = ?
                    AND cs.qsos > 0
                    ORDER BY cs.callsign
                """, (selected_contest, selected_contest))
                callsigns = [{"name": row[0], "qso_count": row[1]} for row in cursor.fetchall()]
        
        return render_template('select_form.html', 
                             contests=contests,
                             selected_contest=selected_contest,
                             selected_callsign=selected_callsign,
                             callsigns=callsigns)
    
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
            # Updated query to include QTH info
            cursor.execute("""
                SELECT COUNT(*) 
                FROM contest_scores cs
                LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
                WHERE cs.contest = ? AND cs.callsign = ?
            """, (contest, callsign))
            if cursor.fetchone()[0] == 0:
                return render_template('error.html', 
                    error=f"No data found for {callsign} in {contest}")

        # Get station data with filters
        stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)

        if stations:
            # Generate HTML content
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

@app.route('/livescore-pilot/api/callsigns')
def get_callsigns():
    contest = request.args.get('contest')
    if not contest:
        return jsonify({"error": "Contest parameter required"}), 400

    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute("""
                SELECT cs.callsign, cs.qsos AS qso_count
                FROM contest_scores cs
                INNER JOIN (
                    SELECT callsign, MAX(timestamp) as max_ts
                    FROM contest_scores
                    WHERE contest = ?
                    GROUP BY callsign
                ) latest ON cs.callsign = latest.callsign 
                    AND cs.timestamp = latest.max_ts
                WHERE cs.contest = ?
                AND cs.qsos > 0
                ORDER BY cs.callsign
            """, (contest, contest))
            callsigns = [{"name": row[0], "qso_count": row[1]} for row in cursor.fetchall()]
            return jsonify(callsigns)
    except Exception as e:
        logger.error(f"Error fetching callsigns: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/livescore-pilot/api/statistics')
def get_statistics():
    """New endpoint to get location statistics"""
    contest = request.args.get('contest')
    
    try:
        with get_db() as db:
            cursor = db.cursor()
            
            query = """
                WITH latest_scores AS (
                    SELECT cs.id
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    ) latest ON cs.timestamp = latest.max_ts
                )
                SELECT
                    COUNT(DISTINCT qi.continent) as continents,
                    COUNT(DISTINCT qi.dxcc_country) as countries,
                    GROUP_CONCAT(DISTINCT qi.continent) as continent_list,
                    COUNT(DISTINCT cs.callsign) as participants
                FROM contest_scores cs
                JOIN latest_scores ls ON cs.id = ls.id
                LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
                WHERE cs.contest = ?
            """
            
            cursor.execute(query, (contest, contest))
            result = cursor.fetchone()
            
            if result:
                stats = {
                    "continents": result[0],
                    "countries": result[1],
                    "continent_list": result[2].split(',') if result[2] else [],
                    "participants": result[3]
                }
                return jsonify(stats)
            else:
                return jsonify({"error": "No statistics available"}), 404
                
    except Exception as e:
        logger.error(f"Error fetching statistics: {str(e)}")
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
            # Updated query to include new location fields
            cursor.execute("""
                SELECT 
                    qi.dxcc_country,
                    qi.dxcc_prefix,
                    qi.continent,
                    qi.cq_zone,
                    qi.iaru_zone, 
                    qi.arrl_section,
                    qi.state_province
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
                'Prefix': row[1],
                'Continent': row[2],
                'CQ Zone': row[3],
                'IARU Zone': row[4],
                'ARRL Section': row[5],
                'State/Province': row[6]
            }

            for filter_type, value in filter_map.items():
                if value:  # Only include non-empty values
                    filters.append({
                        "type": filter_type,
                        "value": value,
                        "label": f"{filter_type}: {value}"
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
