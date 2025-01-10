#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify, make_response, Response
import sqlite3
import os
import logging
import sys
import traceback
import sql_queries
from score_reporter import ScoreReporter
from database_handler import ContestDatabaseHandler
from datetime import datetime
import json
import time
import queue

# Define Config class first
class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'

# Set up detailed logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('logs/debug.log'),
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

        # Queue to receive updates from BatchProcessor
        update_queue = queue.Queue()

        def batch_processor_callback(changed_records):
            """Callback to receive updates from BatchProcessor"""
            try:
                # Filter records for this specific contest/callsign
                relevant_records = [
                    record for record in changed_records
                    if record['contest'] == contest and 
                    (record['callsign'] == callsign or 
                     record['callsign'] in [s['callsign'] for s in ContestDatabaseHandler(get_db()).get_station_details(callsign, contest, filter_type, filter_value)])
                ]
                
                if relevant_records:
                    update_queue.put(relevant_records)
            except Exception as e:
                logger.error(f"Error in batch processor callback: {e}")

        # Get shared BatchProcessor instance
        from batch_processor import shared_processor
        shared_processor.register_callback(batch_processor_callback)

        def generate():
            try:
                # Get initial data
                with get_db() as conn:
                    db_handler = ContestDatabaseHandler(conn)
                    stations = db_handler.get_station_details(callsign, contest, filter_type, filter_value)
                    initial_data = get_formatted_data(stations)
                    yield f"event: init\ndata: {json.dumps(initial_data)}\n\n"

                # Initialize timer for 30-second updates
                next_update = time.time() + 30
                
                while True:
                    current_time = time.time()
                    
                    # Calculate time until next update
                    time_until_update = next_update - time.time()
                    
                    # If it's time for an update
                    if time_until_update <= 0:
                        # Get current station data
                        with get_db() as conn:
                            db_handler = ContestDatabaseHandler(conn)
                            stations = db_handler.get_station_details(callsign, contest, filter_type, filter_value)
                            current_data = get_formatted_data(stations)
                        
                        yield f"event: update\ndata: {json.dumps(current_data)}\n\n"
                        next_update = time.time() + 30
                        time_until_update = 30
                    
                    # Send keep-alive if we're close to connection timeout (25 seconds)
                    if time_until_update > 25:
                        yield ":keep-alive\n\n"
                    
                    # Sleep until next event (update or keep-alive)
                    time.sleep(min(25, time_until_update))
                    
            except Exception as e:
                logger.error(f"Error in SSE stream: {e}")
                logger.error(traceback.format_exc())
            finally:
                # Clean up callback when connection closes
                shared_processor.unregister_callback(batch_processor_callback)
                logger.info(f"SSE connection closed for {callsign} in {contest}")

        return Response(generate(), mimetype='text/event-stream', headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        })

    except Exception as e:
        logger.error("Exception in SSE endpoint:")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

def get_formatted_data(stations_data):
    """
    Format station data for SSE events.
    
    Args:
        stations_data (dict): Dictionary containing station data from ContestDatabaseHandler
        
    Returns:
        dict: Formatted data suitable for SSE events
    """
    formatted_stations = []
    
    # Handle both dictionary and tuple formats for backwards compatibility
    for station in stations_data['stations']:
        if isinstance(station, dict):
            # If station is already a dictionary, use it directly
            formatted_station = {
                "callsign": station.get('callsign'),
                "score": station.get('score', 0),
                "power": station.get('power', ''),
                "assisted": station.get('assisted', ''),
                "category": station.get('category', ''),
                "bandData": station.get('bandData', {}),
                "totalQsos": station.get('totalQsos', 0),
                "multipliers": station.get('multipliers', 0),
                "lastUpdate": station.get('lastUpdate', ''),
                "position": station.get('position', 0),
                "relativePosition": station.get('relativePosition', '')
            }
        else:
            # If station is a tuple, unpack it carefully with defaults
            try:
                station_id = station[0] if len(station) > 0 else None
                callsign = station[1] if len(station) > 1 else ''
                score = station[2] if len(station) > 2 else 0
                power = station[3] if len(station) > 3 else ''
                assisted = station[4] if len(station) > 4 else ''
                timestamp = station[5] if len(station) > 5 else ''
                qsos = station[6] if len(station) > 6 else 0
                mults = station[7] if len(station) > 7 else 0
                position = station[8] if len(station) > 8 else 0
                rel_pos = station[9] if len(station) > 9 else ''
                
                formatted_station = {
                    "callsign": callsign,
                    "score": score,
                    "power": power,
                    "assisted": assisted,
                    "category": get_operator_category(power, assisted),
                    "bandData": get_band_data(station_id) if station_id else {},
                    "totalQsos": qsos,
                    "multipliers": mults,
                    "lastUpdate": timestamp,
                    "position": position,
                    "relativePosition": rel_pos
                }
            except Exception as e:
                logger.error(f"Error formatting station data: {e}")
                logger.debug(f"Station data: {station}")
                # Skip malformed station data
                continue

        formatted_stations.append(formatted_station)

    return {
        "contest": stations_data.get('contest', ''),
        "callsign": stations_data.get('callsign', ''),
        "stations": formatted_stations,
        "timestamp": stations_data.get('timestamp', datetime.utcnow().isoformat())
    }

def get_operator_category(power, assisted):
    """Helper function to determine operator category"""
    if assisted and assisted.upper() == 'ASSISTED':
        return 'SOA'
    return 'SO'

def get_band_data(station_id):
    """Helper function to get band data for a station"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT band, qsos, multipliers
                FROM band_breakdown
                WHERE contest_score_id = ?
            """, (station_id,))
            band_data = {}
            for band, qsos, mults in cursor.fetchall():
                band_data[band] = f"{qsos}/{mults}"
            return band_data
    except Exception as e:
        logger.error(f"Error getting band data: {e}")
        return {}

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
