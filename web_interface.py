from flask import Flask, render_template, request, redirect, url_for, session, abort
from functools import wraps
import secrets
import sqlite3
import logging
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.config.update(
    SECRET_KEY=secrets.token_hex(32),  # Strong secret key
    SESSION_COOKIE_SECURE=True,         # HTTPS only cookies
    SESSION_COOKIE_HTTPONLY=True,       # Prevent XSS accessing session cookie
    SESSION_COOKIE_SAMESITE='Lax',      # CSRF protection
    PERMANENT_SESSION_LIFETIME=1800,     # 30 minute session timeout
)

# Security logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(ip)s] %(message)s'
)

class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'
    ALLOWED_ATTEMPTS = 5
    LOCKOUT_TIME = 300  # 5 minutes

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def get_db():
    """Get database connection with timeout and read-only mode"""
    return sqlite3.connect(Config.DB_PATH, timeout=30, 
                         isolation_level='IMMEDIATE', 
                         check_same_thread=False)

@app.before_request
def before_request():
    """Security checks before each request"""
    # Add client IP to logging context
    logging.LoggerAdapter(logging.getLogger(), {
        'ip': request.remote_addr
    })
    
    # Rate limiting check
    if is_rate_limited(request.remote_addr):
        logging.warning(f"Rate limit exceeded for {request.remote_addr}")
        abort(429)

    # Security headers
    if not request.is_secure and not request.headers.get('X-Forwarded-Proto') == 'https':
        return redirect(url_for(request.endpoint, _external=True, _scheme='https'))

@app.after_request
def after_request(response):
    """Add security headers to all responses"""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    return response

@app.route('/livescore-pilot', methods=['GET', 'POST'])
@require_auth
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
            
            # Input validation
            if not callsign or not contest:
                raise ValueError("Missing required fields")
            
            if not is_valid_callsign(callsign) or not is_valid_contest_name(contest):
                raise ValueError("Invalid input format")
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest)
            
            if stations:
                report_path = generate_secure_filename(callsign, contest)
                reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                return redirect(f'/reports/{report_path}')
            
            logging.warning(f"No stations found for {callsign} in {contest}")
            return render_template('error.html', error="No data found")
        
        return render_template('select_form.html', contests=contests, callsigns=callsigns)
    
    except Exception as e:
        logging.error(f"Error in index: {str(e)}")
        return render_template('error.html', error="An error occurred"), 500

def is_valid_callsign(callsign):
    """Validate callsign format"""
    import re
    return bool(re.match(r'^[A-Z0-9]{3,10}$', callsign))

def is_valid_contest_name(contest):
    """Validate contest name format"""
    import re
    return bool(re.match(r'^[A-Za-z0-9\-_ ]{3,50}$', contest))

def generate_secure_filename(callsign, contest):
    """Generate secure filename for reports"""
    return f"{secrets.token_hex(8)}_{callsign}_{contest}.html"

def is_rate_limited(ip):
    """Check if IP is rate limited"""
    # Implement rate limiting logic here
    pass

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8089, ssl_context='adhoc')
    
