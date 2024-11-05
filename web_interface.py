#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for, session, abort
from functools import wraps
import os
import logging
import secrets
from datetime import timedelta
import bleach
import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash
import re
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from score_reporter import ScoreReporter

# Create Flask application
app = Flask(__name__)

# Configuration class
class Config:
    DB_PATH = os.getenv('LIVESCORE_DB_PATH', '/opt/livescore/contest_data.db')
    OUTPUT_DIR = os.getenv('LIVESCORE_OUTPUT_DIR', '/opt/livescore/reports')
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
    if not SECRET_KEY:
        raise ValueError("No SECRET_KEY set in environment variables!")
    
    # Session configuration
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Strict'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=1)
    MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10MB max-body-size

app.config.from_object(Config)

# Setup logging
logging.basicConfig(
    filename=os.getenv('LIVESCORE_LOG_FILE', 'livescore.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Setup rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

# Secure database connection
class DatabaseConnection:
    def __init__(self):
        self.db_path = app.config['DB_PATH']

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA foreign_keys=ON')
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

def get_db():
    return DatabaseConnection()

# Security headers
@app.after_request
def add_security_headers(response):
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    return response

# Authentication decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Input validation
def validate_contest_input(callsign, contest):
    if not callsign or not contest:
        return False
    callsign_pattern = re.compile(r'^[A-Z0-9/]{3,12}$')
    contest_pattern = re.compile(r'^[A-Z0-9 -]{3,50}$')
    return bool(callsign_pattern.match(callsign) and contest_pattern.match(contest))

# Routes
@app.route('/login', methods=['GET', 'POST'])
@limiter.limit("5 per minute")
def login():
    if request.method == 'POST':
        username = bleach.clean(request.form.get('username', ''))
        password = request.form.get('password', '')
        
        with get_db() as db:
            user = db.execute(
                'SELECT * FROM users WHERE username = ?', (username,)
            ).fetchone()
            
            if user and check_password_hash(user['password'], password):
                session.clear()
                session['user_id'] = user['id']
                session.permanent = True
                return redirect(url_for('index'))
            
            logging.warning(f"Failed login attempt for user: {username}")
        
        return 'Invalid credentials', 401

    return render_template('login.html')

@app.route('/livescore-pilot', methods=['GET', 'POST'])
@login_required
@limiter.limit("20 per minute")
def index():
    try:
        with get_db() as db:
            contests = db.execute(
                "SELECT DISTINCT contest FROM contest_scores ORDER BY contest"
            ).fetchall()
            
            callsigns = db.execute(
                "SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign"
            ).fetchall()
        
        if request.method == 'POST':
            # Validate CSRF token
            if request.form.get('csrf_token') != session.get('csrf_token'):
                logging.warning("CSRF token validation failed")
                abort(403)

            # Clean and validate input
            callsign = bleach.clean(request.form.get('callsign', '').upper())
            contest = bleach.clean(request.form.get('contest', ''))
            
            if not validate_contest_input(callsign, contest):
                logging.warning(f"Invalid input: callsign={callsign}, contest={contest}")
                abort(400, description="Invalid input parameters")
            
            reporter = ScoreReporter(app.config['DB_PATH'])
            stations = reporter.get_station_details(callsign, contest)
            
            if not stations:
                abort(404, description="No data found")
            
            # Secure output directory
            output_dir = app.config['OUTPUT_DIR']
            os.makedirs(output_dir, mode=0o750, exist_ok=True)
            
            reporter.generate_html(callsign, contest, stations, output_dir)
            return redirect('/reports/live.html')
        
        # Generate CSRF token for form
        session['csrf_token'] = secrets.token_hex(32)
        
        return render_template('select_form.html',
                             contests=contests,
                             callsigns=callsigns,
                             csrf_token=session['csrf_token'])
    
    except sqlite3.Error as e:
        logging.error(f"Database error: {str(e)}")
        abort(500)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        abort(500)

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html', error="Resource not found"), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error="Internal server error"), 500

@app.errorhandler(403)
def forbidden_error(error):
    return render_template('error.html', error="Forbidden"), 403

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8089, ssl_context='adhoc')
    
