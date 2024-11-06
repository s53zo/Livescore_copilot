#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, jsonify
import sqlite3
import os
import logging
import traceback

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('/opt/livescore/logs/debug.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

class Config:
    DB_PATH = '/opt/livescore/contest_data.db'

def get_db():
    """Database connection with logging."""
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

    selected_contest = request.form.get('contest')
    selected_callsign = request.form.get('callsign')
    category_scope = request.form.get('category_scope')
    selected_category = None
    contests = []
    callsigns = []

    try:
        # Fetch contests with active station counts
        with get_db() as db:
            cursor = db.cursor()
            logger.debug("Fetching contests with active station counts")
            cursor.execute("""
                SELECT contest, COUNT(DISTINCT callsign) AS active_stations
                FROM contest_scores
                GROUP BY contest
                ORDER BY contest
            """)
            contests = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            logger.debug(f"Found contests: {contests}")

            # If a contest is selected, fetch callsigns for that contest
            if selected_contest:
                logger.debug(f"Fetching callsigns for contest: {selected_contest}")
                cursor.execute("""
                    WITH LatestScores AS (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    )
                    SELECT cs.callsign, COUNT(*) AS qso_count
                    FROM contest_scores cs
                    JOIN LatestScores ls ON cs.callsign = ls.callsign 
                        AND cs.timestamp = ls.max_ts
                    WHERE cs.contest = ?
                    GROUP BY cs.callsign
                    ORDER BY cs.callsign
                """, (selected_contest, selected_contest))
                callsigns = [{"name": row[0], "qso_count": row[1]} for row in cursor.fetchall()]
                logger.debug(f"Found callsigns: {callsigns}")

            # If a callsign is selected and "Selected Callsign's Category Only" is chosen, fetch the category
            if selected_callsign and category_scope == 'selected':
                logger.debug(f"Fetching category for callsign: {selected_callsign}")
                cursor.execute("""
                    SELECT category
                    FROM contest_scores
                    WHERE callsign = ? AND contest = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (selected_callsign, selected_contest))
                category_result = cursor.fetchone()
                if category_result:
                    selected_category = category_result[0]
                    logger.debug(f"Selected callsign's category: {selected_category}")
                else:
                    logger.warning("Selected callsign's category not found")

        # Render the template with context variables
        return render_template(
            'select_form.html',
            contests=contests,
            selected_contest=selected_contest,
            callsigns=callsigns,
            selected_callsign=selected_callsign,
            category_scope=category_scope,
            selected_category=selected_category
        )

    except Exception as e:
        logger.error("Exception in index route:")
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
    logger.info("Starting under gunicorn")
