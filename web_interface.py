#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for, abort
import sqlite3
import os
import logging
import re
from time import time
from score_reporter import ScoreReporter

app = Flask(__name__)

# Configuration
class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'
    # Rate limiting
    MAX_REQUESTS = 30
    WINDOW_SECONDS = 60
    
# Rate limiting storage
request_history = {}

def get_db():
    """Get database connection with security settings"""
    try:
        return sqlite3.connect(Config.DB_PATH, 
                             timeout=5,                    # Connection timeout
                             isolation_level='IMMEDIATE',  # Transaction isolation
                             check_same_thread=False)     # Thread safety
    except sqlite3.Error as e:
        app.logger.error(f"Database connection error: {e}")
        abort(500)

def is_rate_limited(ip):
    """Simple rate limiting"""
    current = time()
    if ip not in request_history:
        request_history[ip] = []
    
    # Clean old requests
    request_history[ip] = [t for t in request_history[ip] 
                          if t > current - Config.WINDOW_SECONDS]
    
    # Add new request
    request_history[ip].append(current)
    
    return len(request_history[ip]) > Config.MAX_REQUESTS

def validate_input(value, pattern):
    """Validate input against pattern"""
    if not value:
        return False
    return bool(re.match(pattern, value))

def secure_query(cursor, query, params=None):
    """Execute query with error handling"""
    try:
        if params:
            return cursor.execute(query, params)
        return cursor.execute(query)
    except sqlite3.Error as e:
        app.logger.error(f"Database query error: {e}")
        abort(500)

@app.before_request
def before_request():
    """Pre-request security checks"""
    # Rate limiting
    if is_rate_limited(request.remote_addr):
        app.logger.warning(f"Rate limit exceeded for {request.remote_addr}")
        abort(429)

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    try:
        with get_db() as db:
            cursor = db.cursor()
            
            # Safe queries with no user input
            secure_query(cursor, "SELECT DISTINCT contest FROM contest_scores ORDER BY contest")
            contests = [row[0] for row in cursor.fetchall()]
            
            secure_query(cursor, "SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign")
            callsigns = [row[0] for row in cursor.fetchall()]
        
        if request.method == 'POST':
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            
            # Input validation
            if not validate_input(callsign, r'^[A-Z0-9]{3,10}$'):
                app.logger.warning(f"Invalid callsign attempt: {callsign} from {request.remote_addr}")
                return render_template('error.html', error="Invalid callsign format"), 400
                
            if not validate_input(contest, r'^[A-Za-z0-9\-_ ]{3,50}$'):
                app.logger.warning(f"Invalid contest attempt: {contest} from {request.remote_addr}")
                return render_template('error.html', error="Invalid contest format"), 400
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest)
            
            if stations:
                reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                return redirect('/reports/live.html')
            
            return render_template('error.html', error="No data found"), 404
        
        return render_template('select_form.html', contests=contests, callsigns=callsigns)
    
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return render_template('error.html', error="An error occurred"), 500

# Error handlers
@app.errorhandler(429)
def too_many_requests(e):
    return render_template('error.html', error="Too many requests. Please try again later."), 429

@app.errorhandler(500)
def server_error(e):
    return render_template('error.html', error="Internal server error"), 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8089)
