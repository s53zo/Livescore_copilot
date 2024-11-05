from flask import Flask, render_template, request, redirect, url_for, abort
import sqlite3
import re
import logging
from functools import wraps

class DatabaseConnection:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        # Enable column access by name
        self.conn.row_factory = sqlite3.Row
        return self.conn
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

class SecureDatabase:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def validate_input(self, value, pattern):
        """Validate input matches expected pattern"""
        if not value or not isinstance(value, str):
            return False
        return bool(re.match(pattern, value))
        
    def get_contests(self):
        """Safely get list of contests"""
        with DatabaseConnection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT contest FROM contest_scores ORDER BY contest")
            return [row['contest'] for row in cursor.fetchall()]
            
    def get_callsigns(self):
        """Safely get list of callsigns"""
        with DatabaseConnection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT callsign FROM contest_scores ORDER BY callsign")
            return [row['callsign'] for row in cursor.fetchall()]
            
    def get_station_details(self, callsign, contest):
        """Get station details with SQL injection protection"""
        # Input validation
        if not self.validate_input(callsign, r'^[A-Z0-9]{3,10}$'):
            logging.warning(f"Invalid callsign format attempted: {callsign}")
            return None
            
        if not self.validate_input(contest, r'^[A-Za-z0-9\-_ ]{3,50}$'):
            logging.warning(f"Invalid contest format attempted: {contest}")
            return None
            
        query = """
        WITH StationScore AS (
            SELECT 
                cs.id, 
                cs.callsign, 
                cs.score, 
                cs.power, 
                cs.assisted,
                cs.timestamp, 
                cs.qsos, 
                cs.multipliers,
                'current' as position,
                1 as rn
            FROM contest_scores cs
            WHERE cs.callsign = ? 
            AND cs.contest = ?
            ORDER BY cs.timestamp DESC
            LIMIT 1
        ),
        NearbyStations AS (
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
                    WHEN cs.score > (SELECT score FROM StationScore) THEN 'above'
                    WHEN cs.score < (SELECT score FROM StationScore) THEN 'below'
                END as position,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        CASE
                            WHEN cs.score > (SELECT score FROM StationScore) THEN 'above'
                            WHEN cs.score < (SELECT score FROM StationScore) THEN 'below'
                        END
                    ORDER BY 
                        CASE
                            WHEN cs.score > (SELECT score FROM StationScore) THEN score END ASC,
                        CASE
                            WHEN cs.score < (SELECT score FROM StationScore) THEN score END DESC
                ) as rn
            FROM contest_scores cs
            WHERE cs.contest = ?
            AND cs.power = (SELECT power FROM StationScore)
            AND cs.assisted = (SELECT assisted FROM StationScore)
            AND cs.callsign != (SELECT callsign FROM StationScore)
            AND cs.timestamp = (
                SELECT MAX(timestamp)
                FROM contest_scores cs2
                WHERE cs2.callsign = cs.callsign
                AND cs2.contest = cs.contest
            )
        )
        SELECT 
            id,
            callsign, 
            score, 
            power, 
            assisted,
            timestamp, 
            qsos, 
            multipliers,
            position,
            rn
        FROM (
            SELECT * FROM StationScore
            UNION ALL
            SELECT * FROM NearbyStations
            WHERE (position = 'above' AND rn <= 2)
            OR (position = 'below' AND rn <= 2)
        )
        ORDER BY score DESC;
        """
        
        try:
            with DatabaseConnection(self.db_path) as conn:
                cursor = conn.cursor()
                # Using parameterized query with parameters passed separately
                cursor.execute(query, (callsign, contest, contest))
                return cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Database error: {str(e)}")
            return None

# Update web_interface.py to use the secure database class
app = Flask(__name__)

@app.route('/livescore-pilot', methods=['GET', 'POST'])
def index():
    try:
        db = SecureDatabase('/opt/livescore/contest_data.db')
        
        if request.method == 'POST':
            callsign = request.form.get('callsign')
            contest = request.form.get('contest')
            
            stations = db.get_station_details(callsign, contest)
            if not stations:
                return render_template('error.html', error="Invalid input or no data found"), 400
                
            reporter = ScoreReporter(Config.DB_PATH)
            success = reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
            if success:
                return redirect('/reports/live.html')
            return render_template('error.html', error="Failed to generate report"), 500
            
        return render_template('select_form.html', 
                             contests=db.get_contests(),
                             callsigns=db.get_callsigns())
                             
    except Exception as e:
        logging.error(f"Error in index: {str(e)}")
        return render_template('error.html', error="An error occurred"), 500
