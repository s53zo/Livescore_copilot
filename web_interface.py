#!/usr/bin/env python3
from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import os
import logging
from score_reporter import ScoreReporter

app = Flask(__name__)

# Configuration
class Config:
    DB_PATH = '/opt/livescore/contest_data.db'
    OUTPUT_DIR = '/opt/livescore/reports'

def get_db():
    return sqlite3.connect(Config.DB_PATH)

@app.route('/livescore-pilot', methods=['GET', 'POST'])
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
            
            reporter = ScoreReporter(Config.DB_PATH)
            stations = reporter.get_station_details(callsign, contest)
            
            if stations:
                reporter.generate_html(callsign, contest, stations, Config.OUTPUT_DIR)
                return redirect('/reports/live.html')
        
        return render_template('select_form.html', contests=contests, callsigns=callsigns)
    
    except Exception as e:
        return str(e), 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8089)
