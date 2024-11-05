from flask import Flask, render_template, request, redirect, url_for, session, abort
from functools import wraps
import secrets
import sqlite3
import os
import logging
from werkzeug.security import generate_password_hash, check_password_hash
from score_reporter import ScoreReporter

app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(32)),
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=1800  # 30 minute session timeout
)

# Configure logging properly for gunicorn
if not app.debug:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'
    MAX_REQUESTS = 30
    WINDOW_SECONDS = 60

# Rate limiting storage
request_history = {}

def get_db():
    """Get database connection with security settings"""
    return sqlite3.connect(Config.DB_PATH, 
                         timeout=30,
                         isolation_level='IMMEDIATE',
                         check_same_thread=False)

def is_rate_limited(ip):
    """Simple rate limiting implementation"""
    from time import time
    current = time()
    if ip not in request_history:
        request_history[ip] = []
    
    # Clean old requests
    request_history[ip] = [t for t in request_history[ip] 
                          if t > current - Config.WINDOW_SECONDS]
    
    # Add new request
    request_history[ip].append(current)
    
    # Check limit
    return len(request_history[ip]) > Config.MAX_REQUESTS

def validate_input(callsign, contest):
    """Validate user input"""
    import re
    if not callsign or not contest:
        return False
    if not re.match(r'^[A-Z0-9]{3,10}$', callsign):
        return False
    if not re.match(r'^[A-Za-z0-9\-_ ]{3,50}$', contest):
        return False
    return True

@app.before_request
def before_request():
    """Security checks before each request"""
    # Log the request with IP
    app.logger.info(f"Request from IP: {request.remote_addr} - Path: {request.path}")
    
    if is_rate_limited(request.remote_addr):
        app.logger.warning(f"Rate limit exceeded for IP: {request.remote_addr}")
        abort(429)  # Too Many Requests
    
    if not request.is_secure and not request.headers.get('X-Forwarded-Proto') == 'https':
        return redirect(url_for(request.endpoint, _external=True, _scheme='https'))

@app.after_request
def after_request(response):
    """Add security headers"""
    response.headers.update({
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
        'Content-Security-Policy': "default-src 'self'"
    })
    return response

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    try:
        with get_db() as db:
            cursor = db.cursor()
            # Use parameterized queries
            cursor.execute("SELECT DISTINCT contest FROM contest_scores ORDER BY contest")
            contests = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign")
            callsigns = [row[0] for row in cursor.fetchall()]
        
        if request.method == 'POST':
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            
            if not validate_input(callsign, contest):
                app.logger.warning(f"Invalid input attempt from {request.remote_addr}: {callsign} - {contest}")
                return render_template('error.html', error="Invalid input format"), 400
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest)
            
            if stations:
                # Generate secure filename
                filename = f"report_{secrets.token_hex(8)}.html"
                
                reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                app.logger.info(f"Generated report for {callsign} - {contest}")
                return redirect(f'/reports/{filename}')
            
            app.logger.info(f"No data found for {callsign} - {contest}")
            return render_template('error.html', error="No data found for the specified criteria"), 404
        
        return render_template('select_form.html', contests=contests, callsigns=callsigns)
    
    except sqlite3.Error as e:
        app.logger.error(f"Database error: {str(e)}")
        return render_template('error.html', error="Database error occurred"), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return render_template('error.html', error="An unexpected error occurred"), 500

@app.errorhandler(429)
def too_many_requests(e):
    return render_template('error.html', error="Too many requests. Please try again later."), 429

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8089)
