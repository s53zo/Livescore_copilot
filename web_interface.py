#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify, make_response
import sqlite3
import os
import logging
import sys
import traceback
from datetime import datetime
from score_reporter import ScoreReporter

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

class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'
    DEBUG = False

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

try:
    app = Flask(__name__)
    logger.info("Flask app created successfully")
except Exception as e:
    logger.error(f"Failed to create Flask app: {str(e)}")
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

@app.route('/livescore-pilot/api/scores')
def get_scores():
    try:
        callsign = request.args.get('callsign')
        contest = request.args.get('contest')
        filter_type = request.args.get('filter_type', 'none')
        filter_value = request.args.get('filter_value', 'none')

        if not (callsign and contest):
            return jsonify({"error": "Missing required parameters"}), 400

        reporter = ScoreReporter(Config.DB_PATH)
        stations = reporter.get_station_details(callsign, contest, filter_type, filter_value)

        if not stations:
            return jsonify({"error": "No data found"}), 404

        # Transform data for frontend
        formatted_stations = []
        for station in stations:
            band_data = reporter.get_band_breakdown_with_rates(
                station[0],  # station_id
                station[1],  # callsign
                contest,
                station[5]   # timestamp
            )

            formatted_stations.append({
                "callsign": station[1],
                "score": station[2],
                "power": station[3],
                "assisted": station[4],
                "timestamp": station[5],
                "qsos": station[6],
                "multipliers": station[7],
                "bandData": band_data
            })

        return jsonify({
            "contest": contest,
            "callsign": callsign,
            "timestamp": datetime.utcnow().isoformat(),
            "stations": formatted_stations
        })

    except Exception as e:
        logger.error(f"Error in get_scores: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route('/reports/live.html')
def live_report():
    """Handle live report requests"""
    try:
        callsign = request.args.get('callsign')
        contest = request.args.get('contest')
        filter_type = request.args.get('filter_type', 'none')
        filter_value = request.args.get('filter_value', 'none')

        if not (callsign and contest):
            return render_template('error.html', error="Missing required parameters")

        logger.info(f"Generating report for: {contest}, {callsign}, filter:{filter_type}={filter_value}")

        with get_db() as conn:
            cursor = conn.cursor()

            # First verify that we have data
            cursor.execute("""
                SELECT COUNT(*)
                FROM contest_scores cs
                JOIN qth_info qi ON qi.contest_score_id = cs.id
                WHERE cs.contest = ? AND qi.arrl_section = ?
            """, (contest, filter_value))
            count = cursor.fetchone()[0]
            logger.debug(f"Found {count} records for section {filter_value}")

            # Use the working query
            query = """
            WITH ranked_scores AS (
                SELECT cs.id,
                       cs.callsign,
                       cs.score,
                       cs.power,
                       cs.assisted,
                       cs.timestamp,
                       cs.qsos,
                       cs.multipliers,
                       ROW_NUMBER() OVER (PARTITION BY cs.callsign ORDER BY cs.timestamp DESC) as rn
                FROM contest_scores cs
                WHERE cs.contest = ?
            ),
            filtered_scores AS (
                SELECT rs.*
                FROM ranked_scores rs
                JOIN qth_info qi ON qi.contest_score_id = rs.id
                WHERE rs.rn = 1
                AND qi.arrl_section = ?
            ),
            reference_score AS (
                SELECT score
                FROM filtered_scores
                WHERE callsign = ?
            )
            SELECT 
                fs.id,
                fs.callsign,
                fs.score,
                fs.power,
                fs.assisted,
                fs.timestamp,
                fs.qsos,
                fs.multipliers,
                CASE 
                    WHEN fs.callsign = ? THEN 'current'
                    WHEN fs.score > (SELECT score FROM reference_score) THEN 'above'
                    ELSE 'below'
                END as position,
                ROW_NUMBER() OVER (ORDER BY fs.score DESC) as rank
            FROM filtered_scores fs
            ORDER BY fs.score DESC
            """

            cursor.execute(query, (contest, filter_value, callsign, callsign))
            results = cursor.fetchall()

            if not results:
                if request.headers.get('Accept') == 'application/json':
                    return jsonify({"error": "No data found"}), 404
                return render_template('error.html', error="No data found for the selected criteria")

            # Format results for JSON response
            stations = []
            for row in results:
                station_id, station_call, score, power, assisted, timestamp, qsos, mults, position, rank = row
                
                # Get band breakdown
                cursor.execute("""
                    SELECT bb.band,
                           bb.qsos,
                           bb.multipliers
                    FROM band_breakdown bb
                    WHERE bb.contest_score_id = ?
                    ORDER BY bb.band
                """, (station_id,))
                
                band_data = {}
                for band_row in cursor.fetchall():
                    band, band_qsos, band_mults = band_row
                    band_data[band] = [band_qsos, band_mults, 0, 0]  # Include placeholders for rates

                stations.append({
                    "id": station_id,
                    "callsign": station_call,
                    "score": score,
                    "power": power,
                    "assisted": assisted,
                    "timestamp": timestamp,
                    "qsos": qsos,
                    "multipliers": mults,
                    "position": position,
                    "rank": rank,
                    "bandData": band_data,
                    "totalRates": {"long": 0, "short": 0}
                })

            response_data = {
                "contest": contest,
                "callsign": callsign,
                "filterType": filter_type,
                "filterValue": filter_value,
                "timestamp": datetime.utcnow().isoformat(),
                "stations": stations
            }

            if request.headers.get('Accept') == 'application/json':
                return jsonify(response_data)
            
            # For HTML response, use the template
            with open(os.path.join(os.path.dirname(__file__), 'templates', 'score_template.html'), 'r') as f:
                template = f.read()
                return template.format(
                    contest=contest,
                    callsign=callsign,
                    filter_type=filter_type,
                    filter_value=filter_value,
                    additional_css='',
                    timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                )

    except Exception as e:
        logger.error(f"Error in live_report: {e}")
        logger.error(traceback.format_exc())
        if request.headers.get('Accept') == 'application/json':
            return jsonify({"error": str(e)}), 500
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

if __name__ == '__main__':
    logger.info("Starting development server")
    app.run(host='127.0.0.1', port=8089)
else:
    # When running under gunicorn
    logger.info("Starting under gunicorn")
