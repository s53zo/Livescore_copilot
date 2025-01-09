#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify, make_response, Response
import sqlite3
import os
import logging
import sys
import traceback
import sql_queries
from score_reporter import ScoreReporter
from datetime import datetime
import json
import time

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
            cursor.execute(sql_queries.GET_CONTESTS)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            # Get contest and callsign from form or query parameters
            selected_contest = request.form.get('contest') or request.args.get('contest')
            selected_callsign = request.form.get('callsign') or request.args.get('callsign')
            
            callsigns = []
            
            if selected_contest:
                # Fetch unique callsigns with their latest QSO count for the selected contest
                cursor.execute(sql_queries.GET_CALLSIGNS, (selected_contest, selected_contest))
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
            cursor.execute(sql_queries.VERIFY_STATION, (contest, callsign))
            if cursor.fetchone()[0] == 0:
                return render_template('error.html', 
                    error=f"No data found for {callsign} in {contest}")

        # Get station data with filters
        position_filter = request.args.get('position_filter', 'all')
        stations = reporter.get_station_details(
            callsign, 
            contest, 
            filter_type, 
            filter_value,
            position_filter
        )

        if stations:
            # Generate HTML content directly
            template_path = os.path.join(os.path.dirname(__file__), 'templates', 'score_template.html')
            with open(template_path, 'r') as f:
                template = f.read()

            html_content = reporter.generate_html_content(
                template, 
                callsign, 
                contest, 
                stations,
                filter_type,
                filter_value,
                position_filter
            )
            
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
            cursor.execute(sql_queries.API_GET_CONTESTS)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            return jsonify(contests)
    except Exception as e:
        logger.error(f"Error fetching contests: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/livescore-pilot/events')
def sse_endpoint():
    try:
        # Get parameters from URL
        callsign = request.args.get('callsign')
        contest = request.args.get('contest')
        filter_type = request.args.get('filter_type', 'none')
        filter_value = request.args.get('filter_value', 'none')

        if not (callsign and contest):
            return jsonify({"error": "Missing required parameters"}), 400

        def get_formatted_data(stations_data):
            formatted_stations = []
            for station in stations_data:
                station_id, call, score, power, assisted, timestamp, qsos, mults, pos, rel_pos = station
                
                # Get band breakdown data
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT bb.band, bb.qsos, bb.points, bb.multipliers
                        FROM band_breakdown bb
                        JOIN contest_scores cs ON bb.contest_score_id = cs.id
                        WHERE cs.id = ?
                    """, (station_id,))
                    
                    band_data = {}
                    for band, qsos, points, band_mults in cursor.fetchall():
                        band_data[band] = f"{qsos}/{band_mults}"
                
                # Determine operator category
                op_category = "SO"  # Default
                if assisted == "ASSISTED":
                    op_category = "SOA"
                elif assisted == "MULTI-SINGLE":
                    op_category = "M/S"
                elif assisted == "MULTI-MULTI":
                    op_category = "M/M"

                formatted_station = {
                    "callsign": call,
                    "score": score,
                    "power": power,
                    "assisted": assisted,
                    "category": op_category,
                    "bandData": band_data,
                    "totalQsos": qsos,
                    "multipliers": mults,
                    "lastUpdate": timestamp,
                    "position": pos,
                    "relativePosition": rel_pos
                }
                formatted_stations.append(formatted_station)

            return {
                "contest": contest,
                "callsign": callsign,
                "stations": formatted_stations,
                "timestamp": datetime.now().isoformat()
            }

        def generate():
            # Get initial data
            with get_db() as conn:
                cursor = conn.cursor()
                stations = get_station_details(
                    conn, callsign, contest, filter_type, filter_value
                )
                initial_data = get_formatted_data(stations)
                yield f"event: init\ndata: {json.dumps(initial_data)}\n\n"

                last_timestamp = None

                while True:
                    current_stations = get_station_details(
                        conn, callsign, contest, filter_type, filter_value
                    )
                    current_data = get_formatted_data(current_stations)
                    
                    # Only send update if data has changed
                    if current_data != last_timestamp:
                        yield f"event: update\ndata: {json.dumps(current_data)}\n\n"
                        last_timestamp = current_data
                    
                    yield ":keep-alive\n\n"
                    time.sleep(1)

        response = Response(generate(), mimetype='text/event-stream')
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Connection'] = 'keep-alive'
        return response

    except Exception as e:
        logger.error("Exception in SSE endpoint:")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

def get_station_details(conn, callsign, contest, filter_type, filter_value):
    cursor = conn.cursor()
    query = """
    WITH ranked_stations AS (
        SELECT 
            cs.id,
            cs.callsign,
            cs.score,
            cs.power,
            cs.assisted,
            cs.timestamp,
            cs.qsos,
            cs.multipliers,
            ROW_NUMBER() OVER (ORDER BY cs.score DESC) as position
        FROM contest_scores cs
        JOIN qth_info qi ON qi.contest_score_id = cs.id
        WHERE cs.contest = ?
        AND cs.id IN (
            SELECT MAX(id)
            FROM contest_scores
            WHERE contest = ?
            GROUP BY callsign
        )
    """
    
    params = [contest, contest]
    
    if filter_type and filter_value and filter_type.lower() != 'none':
        filter_map = {
            'DXCC': 'qi.dxcc_country',
            'CQ Zone': 'qi.cq_zone',
            'IARU Zone': 'qi.iaru_zone',
            'ARRL Section': 'qi.arrl_section',
            'State/Province': 'qi.state_province',
            'Continent': 'qi.continent'
        }
        
        if field := filter_map.get(filter_type):
            query += f" AND {field} = ?"
            params.append(filter_value)
    
    query += """
    ) SELECT 
        id, 
        callsign,
        score,
        power,
        assisted,
        timestamp,
        qsos,
        multipliers,
        position,
        CASE 
            WHEN callsign = ? THEN 'current'
            WHEN score > (SELECT score FROM ranked_stations WHERE callsign = ?) 
            THEN 'above' 
            ELSE 'below' 
        END as relative_pos
    FROM ranked_stations
    ORDER BY score DESC
    """
    
    params.extend([callsign, callsign])
    cursor.execute(query, params)
    return cursor.fetchall()

@app.route('/livescore-pilot/api/callsigns')
def get_callsigns():
    contest = request.args.get('contest')
    if not contest:
        return jsonify({"error": "Contest parameter required"}), 400

    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute(sql_queries.API_GET_CALLSIGNS, (contest, contest))
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
            cursor.execute(sql_queries.GET_FILTERS, (contest, callsign))
            
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

if __name__ == '__main__':
    logger.info("Starting development server")
    app.run(host='127.0.0.1', port=8089)
else:
    # When running under gunicorn
    logger.info("Starting under gunicorn")
