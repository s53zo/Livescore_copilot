#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, send_from_directory, jsonify
from flask_cors import CORS
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
    CORS(app)
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

@app.route('/api/contests')
def get_contests():
    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute("""
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                )
                SELECT 
                    contest, 
                    COUNT(DISTINCT callsign) as active_stations
                FROM latest_scores
                GROUP BY contest
                ORDER BY contest
            """)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            logger.debug(f"Fetched contests: {contests}")
            return jsonify(contests)
    except Exception as e:
        logger.error(f"Error getting contests: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/api/callsigns')
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
                ORDER BY cs.callsign
            """, (contest, contest))
            callsigns = [{"name": row[0], "qso_count": row[1]} for row in cursor.fetchall()]
            logger.debug(f"Fetched callsigns for {contest}: {callsigns}")
            return jsonify(callsigns)
    except Exception as e:
        logger.error(f"Error getting callsigns: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/api/filters')
def get_filters():
    contest = request.args.get('contest')
    callsign = request.args.get('callsign')
    
    if not (contest and callsign):
        return jsonify({"error": "Contest and callsign parameters required"}), 400
    
    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute("""
                SELECT 
                    qi.dxcc_country,
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
            field_names = ['dxcc_country', 'cq_zone', 'iaru_zone', 'arrl_section', 'state_province']
            
            for field, value in zip(field_names, row):
                if value:  # Only add non-empty values
                    filters.append({
                        "type": field,
                        "value": value
                    })
            
            logger.debug(f"Fetched filters for {callsign} in {contest}: {filters}")
            return jsonify(filters)
    except Exception as e:
        logger.error(f"Error getting filters: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

def get_filtered_stations(contest, callsign, filter_type=None, filter_value=None):
    """Get station details with filtering"""
    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Base query with filtering
            query = """
                WITH station_scores AS (
                    SELECT cs.*,
                           ROW_NUMBER() OVER (ORDER BY cs.score DESC) as rank
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.timestamp = latest.max_ts
                    LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
                    WHERE cs.contest = ?
            """
            
            params = [contest, contest]
            
            # Add filter condition if specified
            if filter_type and filter_type != 'none' and filter_value and filter_value != 'none':
                query += f" AND qi.{filter_type} = ?"
                params.append(filter_value)
                
            query += """)
                SELECT 
                    cs.id,
                    cs.callsign,
                    cs.score,
                    cs.power,
                    cs.assisted,
                    cs.timestamp,
                    cs.qsos,
                    cs.multipliers,
                    CASE 
                        WHEN cs.callsign = ? THEN 'current'
                        WHEN cs.score > (SELECT score FROM station_scores WHERE callsign = ?) THEN 'above'
                        ELSE 'below'
                    END as position,
                    ROW_NUMBER() OVER (ORDER BY cs.score DESC) as display_rank
                FROM station_scores cs
                ORDER BY cs.score DESC"""
            
            params.extend([callsign, callsign])
            
            logger.debug(f"Executing query with params: {params}")
            cursor.execute(query, params)
            results = cursor.fetchall()
            logger.debug(f"Found {len(results)} stations")
            return results
            
    except Exception as e:
        logger.error(f"Error getting filtered stations: {str(e)}")
        logger.error(traceback.format_exc())
        return None

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    logger.debug(f"Request received: {request.method}")
    
    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Get contests with station counts using the same query as the API
            cursor.execute("""
                WITH latest_scores AS (
                    SELECT cs.id, cs.callsign, cs.contest
                    FROM contest_scores cs
                    INNER JOIN (
                        SELECT callsign, contest, MAX(timestamp) as max_ts
                        FROM contest_scores
                        GROUP BY callsign, contest
                    ) latest ON cs.callsign = latest.callsign 
                        AND cs.contest = latest.contest
                        AND cs.timestamp = latest.max_ts
                )
                SELECT 
                    contest, 
                    COUNT(DISTINCT callsign) as active_stations
                FROM latest_scores
                GROUP BY contest
                ORDER BY contest
            """)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            selected_contest = request.form.get('contest') or request.args.get('contest')
            selected_callsign = request.form.get('callsign') or request.args.get('callsign')
            
            callsigns = []
            
            if selected_contest:
                # Fetch callsigns with QSO count
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
        callsign = request.args.get('callsign')
        contest = request.args.get('contest')
        filter_type = request.args.get('filter_type', 'none')
        filter_value = request.args.get('filter_value', 'none')

        if not (callsign and contest):
            return render_template('error.html', error="Missing required parameters")

        logger.info(f"Generating report: callsign={callsign}, contest={contest}, "
                   f"filter_type={filter_type}, filter_value={filter_value}")

        stations = get_filtered_stations(contest, callsign, filter_type, filter_value)

        if stations:
            reporter = ScoreReporter(Config.DB_PATH)
            success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
            
            if success:
                response = send_from_directory(Config.OUTPUT_DIR, 'live.html')
                response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                return response
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
