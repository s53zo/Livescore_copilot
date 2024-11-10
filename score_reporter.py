#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime
import sys
from rate_calculator import RateCalculator
from qso_rate import QsoRateCalculator 

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None, rate_minutes=60):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_calculator = QsoRateCalculator(self.db_path)
        self.setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}")

    def setup_logging(self):
        """Setup logging configuration with both file and console handlers"""
        try:
            # Create logger
            self.logger = logging.getLogger('ScoreReporter')
            self.logger.setLevel(logging.DEBUG)
            
            # Clear any existing handlers
            if self.logger.handlers:
                self.logger.handlers.clear()
            
            # Create logs directory if it doesn't exist
            log_dir = '/opt/livescore/logs'
            os.makedirs(log_dir, exist_ok=True)
            
            # Create formatters
            detailed_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            )
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            
            # File handler for detailed debugging
            debug_log = os.path.join(log_dir, 'score_reporter.log')
            file_handler = logging.FileHandler(debug_log)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(detailed_formatter)
            
            # Console handler for basic info
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(console_formatter)
            
            # Add handlers to logger
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
            
            # Log startup message
            self.logger.info("Score Reporter logging initialized")
            self.logger.debug(f"Debug log file: {debug_log}")
            
        except Exception as e:
            print(f"Error setting up logging: {e}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            raise

    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None):
        """Get station details and all competitors with optional filtering"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
    
                # Base query with QTH info join
                query = """
                    WITH latest_scores AS (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    ),
                    station_scores AS (
                        SELECT 
                            cs.*,
                            qi.dxcc_country,
                            qi.cq_zone,
                            qi.iaru_zone,
                            qi.arrl_section,
                            qi.state_province,
                            qi.continent
                        FROM contest_scores cs
                        INNER JOIN latest_scores ls 
                            ON cs.callsign = ls.callsign 
                            AND cs.timestamp = ls.max_ts
                        LEFT JOIN qth_info qi 
                            ON qi.contest_score_id = cs.id
                        WHERE cs.contest = ?
                        AND cs.qsos > 0
                """
                
                params = [contest, contest]
    
                # Add filter conditions if specified
                if filter_type and filter_value and filter_type.lower() != 'none':
                    filter_map = {
                        'DXCC': 'dxcc_country',
                        'CQ Zone': 'cq_zone',
                        'IARU Zone': 'iaru_zone',
                        'ARRL Section': 'arrl_section',
                        'State/Province': 'state_province',
                        'Continent': 'continent'
                    }
                    
                    db_field = filter_map.get(filter_type)
                    if db_field:
                        query += f" AND qi.{db_field} = ?"
                        params.append(filter_value)
    
                # Complete the query
                query += """)
                    SELECT 
                        ss.id,
                        ss.callsign,
                        ss.score,
                        ss.power,
                        ss.assisted,
                        ss.timestamp,
                        ss.qsos,
                        ss.multipliers,
                        CASE 
                            WHEN ss.callsign = ? THEN 'current'
                            WHEN ss.score > (SELECT score FROM station_scores WHERE callsign = ?) THEN 'above'
                            ELSE 'below'
                        END as position,
                        ROW_NUMBER() OVER (ORDER BY ss.score DESC) as rn
                    FROM station_scores ss
                    ORDER BY ss.score DESC
                """
                
                params.extend([callsign, callsign])
                cursor.execute(query, params)
                return cursor.fetchall()
                    
        except Exception as e:
            self.logger.error(f"Error in get_station_details: {e}")
            self.logger.error(traceback.format_exc())
            return None

    def get_band_breakdown_with_rates(self, station_id, callsign, contest, timestamp):
        """Get band breakdown with both 60-minute and 15-minute rates"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                return self.rate_calculator.calculate_band_rates(
                    cursor, callsign, contest, timestamp
                )
        except Exception as e:
            self.logger.error(f"Error in get_band_breakdown_with_rates: {e}")
            self.logger.error(traceback.format_exc())
            return {}

    def get_total_rates(self, station_id, callsign, contest, timestamp):
        """Get total QSO rates for both time windows"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                return self.rate_calculator.calculate_rates(
                    cursor, callsign, contest, timestamp
                )
        except Exception as e:
            self.logger.error(f"Error in get_total_rates: {e}")
            self.logger.error(traceback.format_exc())
            return 0, 0

    @staticmethod
    def format_band_data(band_data, reference_rates=None, band=None):
        """Format band data as QSO/Mults (60h/15h) with rate comparison"""
        if band_data:
            qsos, mults, long_rate, short_rate = band_data
            if qsos > 0:
                # Get reference rates for this band
                ref_short_rate = 0
                if reference_rates and band in reference_rates:
                    _, _, _, ref_short_rate = reference_rates[band]
                
                # Determine if 15-minute rate is better
                better_rate = short_rate > ref_short_rate
                
                # Format rates
                long_rate_str = f"{long_rate:+d}" if long_rate != 0 else "0"
                short_rate_str = f"{short_rate:+d}" if short_rate != 0 else "0"
                
                # Apply CSS class based on 15-minute rate comparison
                rate_class = "better-rate" if better_rate else "worse-rate"
                
                return f'{qsos}/{mults} (<span style="color: gray;">{long_rate_str}</span>/<span class="{rate_class}">{short_rate_str}</span>)'
        return "-/- (0/0)"
    
    def format_total_data(self, qsos, mults, long_rate, short_rate, reference_long_rate=0, reference_short_rate=0):
        """Format total QSO/Mults with both rates, coloring short rate based on comparison"""
        # Format the rates with + sign if positive
        long_rate_str = f"{long_rate:+d}" if long_rate > 0 else "0"
        short_rate_str = f"{short_rate:+d}" if short_rate > 0 else "0"
        
        # Determine if current station's rate is better than reference
        better_rate = short_rate > reference_short_rate
        
        # Apply rate class based on comparison
        rate_class = "better-rate" if better_rate else "worse-rate"
        
        return f'{qsos}/{mults} (<span style="color: gray;">{long_rate_str}</span>/<span class="{rate_class}">{short_rate_str}</span>)'

    @staticmethod
    def get_operator_category(operator, transmitter, assisted):
        """Map operation categories based on defined rules"""
        # Handle empty/NULL assisted value - default to NON-ASSISTED
        assisted = assisted if assisted else 'NON-ASSISTED'
