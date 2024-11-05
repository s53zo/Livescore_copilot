#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import sqlite3
import os
import logging
from logging.handlers import RotatingFileHandler
import re
from score_reporter import ScoreReporter

app = Flask(__name__)

# Set up rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["100 per day", "10 per minute"]
)

# Configure logging
if not os.path.exists('logs'):
    os.makedirs('logs')

file_handler = RotatingFileHandler(
    'logs/web_interface.log',
    maxBytes=10240,
    backupCount=10
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('Web interface startup')

# Configuration
class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'

def get_db():
    """Get database connection with timeout and error handling"""
    try:
        db = sqlite3.connect(
            Config.DB_PATH,
            timeout=30,
            isolation_level=None
        )
        db.row_factory = sqlite3.Row
        return db
    except sqlite3.Error as e:
        app.logger.error(f"Database connection error: {e}")
        abort(500)

def validate_input(callsign, contest):
    """Validate user input"""
    if not callsign or not contest:
        return False
    
    if not re.match(r'^[A-Z0-9\/]{3,}$', callsign):
        return False
    
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute(
        "SELECT 1 FROM contest_scores WHERE callsign = ? AND contest = ? LIMIT 1",
        (callsign, contest)
    )
    
    return cursor.fetchone() is not None

@app.route('/livescore-pilot', methods=['GET', 'POST'])
@limiter.limit("10 per minute")
def index():
    try:
        with get_db() as db:
            cursor = db.cursor()
            cursor.execute("SELECT DISTINCT contest FROM contest_scores ORDER BY contest")
            contests = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign")
            callsigns = [row[0] for row in cursor.fetchall()]
        
        if request.method == 'POST':
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            
            if not validate_input(callsign, contest):
                app.logger.warning(f"Invalid input attempt: {callsign}, {contest}")
                abort(400)
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest)
            
            if not stations:
                app.logger.error(f"No data found for {callsign} in {contest}")
                abort(404)
            
            if not reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR):
                app.logger.error("Failed to generate report")
                abort(500)
            
            return redirect('/reports/live.html')
        
        return render_template('select_form.html', contests=contests, callsigns=callsigns)
    
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}")
        abort(500)

@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html', error="Resource not found"), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error="Internal server error"), 500

@app.errorhandler(429)
def ratelimit_error(error):
    return render_template('error.html', error="Too many requests. Please try again later."), 429
