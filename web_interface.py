#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify, make_response
from flask_socketio import SocketIO, join_room, leave_room
import sqlite3
import os
import re # For sanitizing room names
import logging
import sys
import traceback
from score_reporter import ScoreReporter
from datetime import datetime

# Define Config class first
class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'

# Set up detailed logging
logging.basicConfig(
    level=logging.ERROR,
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
    # Create Flask app
    app = Flask(__name__)
    logger.info("Flask app created successfully")
    # Initialize SocketIO
    # Make socketio global so batch_processor can potentially access it (consider better patterns later)
    socketio = SocketIO(app, async_mode='gevent', cors_allowed_origins="*") # Added cors_allowed_origins for flexibility
    logger.info("Flask-SocketIO initialized successfully")

except Exception as e:
    logger.error(f"Failed to create Flask app")
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
                # Fetch unique callsigns with their latest QSO count for the selected contest
                cursor.execute("""
                    WITH latest_scores AS (
                        SELECT cs.callsign, cs.qsos, cs.timestamp
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
                    )
                    SELECT DISTINCT callsign, qsos as qso_count
                    FROM latest_scores
                    ORDER BY callsign
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


# --- New API Endpoint for Latest Score ---
def _sanitize_room_name(name):
    """Remove potentially problematic characters for room names."""
    return re.sub(r'[^a-zA-Z0-9_-]', '_', name)

@app.route('/livescore-pilot/api/latest_score/<contest>/<callsign>')
def get_latest_score(contest, callsign):
    """Fetches the latest score, band breakdown, and QTH info for a callsign/contest."""
    logger.debug(f"API request for latest score: {contest}/{callsign}")
    try:
        with get_db() as conn:
            cursor = conn.cursor()

            # Find the latest score entry ID
            cursor.execute("""
                SELECT id, timestamp, score, qsos, multipliers, power, assisted, transmitter, ops, bands, mode, club, section, points
                FROM contest_scores
                WHERE contest = ? AND callsign = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (contest, callsign))
            score_row = cursor.fetchone()

            if not score_row:
                logger.warning(f"No score data found for {contest}/{callsign}")
                return jsonify({"error": "No score data found"}), 404

            score_id, timestamp, score, qsos, multipliers, power, assisted, transmitter, ops, bands, mode, club, section, points = score_row

            # Fetch band breakdown for this score ID
            cursor.execute("""
                SELECT band, mode, qsos, points, multipliers
                FROM band_breakdown
                WHERE contest_score_id = ?
            """, (score_id,))
            band_rows = cursor.fetchall()
            band_breakdown_dict = {}
            for band, b_mode, b_qsos, b_points, b_mults in band_rows:
                 band_key = f"{band}m" # Match format used in mqtt_distributor
                 band_breakdown_dict[band_key] = {
                     "mode": b_mode,
                     "qsos": b_qsos,
                     "points": b_points,
                     "mults": b_mults
                 }


            # Fetch QTH info for this score ID
            cursor.execute("""
                SELECT dxcc_country, cq_zone, iaru_zone, arrl_section, state_province, grid6, continent
                FROM qth_info
                WHERE contest_score_id = ?
            """, (score_id,))
            qth_row = cursor.fetchone()
            qth_info_dict = {}
            if qth_row:
                dxcc, cqz, ituz, arrl_sec, state, grid, continent = qth_row
                qth_info_dict = {
                    "dxcc": dxcc,
                    "cqz": cqz,
                    "ituz": ituz,
                    "section": arrl_sec or section, # Use score section if qth section is empty
                    "state": state,
                    "grid": grid,
                    "continent": continent
                }

            latest_data = {
                "timestamp": timestamp,
                "contest": contest,
                "callsign": callsign,
                "score": score,
                "qsos": qsos,
                "multipliers": multipliers,
                "points": points,
                "power": power,
                "assisted": assisted,
                "transmitter": transmitter,
                "ops": ops,
                "bands_class": bands, # Renamed to avoid clash with breakdown
                "mode": mode,
                "club": club,
                "section": section, # From main score table
                "bands": band_breakdown_dict, # Band breakdown details
                "qth": qth_info_dict
            }

            logger.debug(f"Returning latest score data for {contest}/{callsign}")
            return jsonify(latest_data)

    except Exception as e:
        logger.error(f"Error fetching latest score for {contest}/{callsign}: {e}")
        logger.error(traceback.format_exc())
        return jsonify({"error": "Internal server error"}), 500


# --- SocketIO Event Handlers ---

@socketio.on('connect')
def handle_connect():
    logger.info(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    # Rooms are left automatically by Flask-SocketIO
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('join_room')
def handle_join_room(data):
    """Client joins a room based on contest and callsign"""
    contest = data.get('contest')
    callsign = data.get('callsign')
    if contest and callsign:
        # Sanitize inputs before creating room name
        safe_contest = _sanitize_room_name(contest)
        safe_callsign = _sanitize_room_name(callsign)
        room = f"{safe_contest}_{safe_callsign}"
        join_room(room)
        logger.info(f"Client {request.sid} joined room: {room}")
    else:
        logger.warning(f"Client {request.sid} sent invalid join_room data: {data}")


# --- Error Handlers ---

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
                WITH latest_scores AS (
                    SELECT cs.callsign, cs.qsos, cs.timestamp
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
                )
                SELECT DISTINCT callsign, qsos as qso_count
                FROM latest_scores
                ORDER BY callsign
            """, (contest, contest))
            callsigns = [{"name": row[0], "qso_count": row[1]} for row in cursor.fetchall()]
            return jsonify(callsigns)
    except Exception as e:
        logger.error(f"Error fetching callsigns: {str(e)}")
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
                       qi.arrl_section, qi.state_province, qi.continent
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
                'State/Province': row[4],
                'Continent': row[5]
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

# --- Main Execution Logic ---

# Remove the old __main__ block, startup will be handled by socketio.run or gunicorn
# if __name__ == '__main__':
#     logger.info("Starting development server with SocketIO")
#     # Use socketio.run for development
#     socketio.run(app, host='127.0.0.1', port=8089, debug=True) # Add debug=True for development ease

# Keep the gunicorn log message if needed
# else:
#     # When running under gunicorn
#     logger.info("Starting under gunicorn with SocketIO")
