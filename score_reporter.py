#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime, timedelta
from flask import request
import sys
from sql_queries import (CALCULATE_RATES, CALCULATE_BAND_RATES, GET_STATION_DETAILS,
                        GET_BAND_BREAKDOWN, GET_BAND_BREAKDOWN_WITH_RATES,
                        GET_FILTERS, INSERT_CONTEST_DATA, INSERT_BAND_BREAKDOWN,
                        INSERT_QTH_INFO)

class RateCalculator:
    def __init__(self, db_path, debug=False):
        self.db_path = db_path
        self.debug = debug
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger('RateCalculator')
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            self.logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def calculate_rates(self, cursor, callsign, contest, timestamp, long_window=60, short_window=15):
        """Calculate QSO rates using centralized SQL query"""
        try:
            current_ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            # Calculate long window rate
            long_window_start = current_ts - timedelta(minutes=long_window)
            cursor.execute(CALCULATE_RATES, (callsign, contest, 
                           long_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                           current_ts.strftime('%Y-%m-%d %H:%M:%S'),
                           long_window_start.strftime('%Y-%m-%d %H:%M:%S')))
            row = cursor.fetchone()
            long_rate = int(round(row[0] * 60 / long_window)) if row and row[0] else 0
    
            # Calculate short window rate
            short_window_start = current_ts - timedelta(minutes=short_window) 
            cursor.execute(CALCULATE_RATES, (callsign, contest,
                           short_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                           current_ts.strftime('%Y-%m-%d %H:%M:%S'),
                           short_window_start.strftime('%Y-%m-%d %H:%M:%S')))
            row = cursor.fetchone()
            short_rate = int(round(row[0] * 60 / short_window)) if row and row[0] else 0
    
            return long_rate, short_rate
                
        except Exception as e:
            self.logger.error(f"Error calculating rates: {e}")
            self.logger.debug(traceback.format_exc())
            return 0, 0
    
    def calculate_band_rates(self, cursor, callsign, contest, timestamp, long_window=60, short_window=15):
        """Calculate per-band QSO rates using centralized SQL query"""
        try:
            current_ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            
            # Get current band data
            cursor.execute(GET_BAND_BREAKDOWN, (callsign, contest, timestamp))
            band_data = {row[0]: [row[1], row[2], 0, 0] for row in cursor.fetchall()}
    
            # Calculate long window rates
            long_window_start = current_ts - timedelta(minutes=long_window)
            cursor.execute(CALCULATE_BAND_RATES, (callsign, contest, 
                           long_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                           current_ts.strftime('%Y-%m-%d %H:%M:%S'),
                           long_window_start.strftime('%Y-%m-%d %H:%M:%S')))
            for row in cursor.fetchall():
                band = row[0]
                if band in band_data:
                    band_data[band][2] = int(round(row[1] * 60 / long_window))
            
            # Calculate short window rates
            short_window_start = current_ts - timedelta(minutes=short_window)
            cursor.execute(CALCULATE_BAND_RATES, (callsign, contest,
                           short_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                           current_ts.strftime('%Y-%m-%d %H:%M:%S'),
                           short_window_start.strftime('%Y-%m-%d %H:%M:%S')))
            for row in cursor.fetchall():
                band = row[0]
                if band in band_data:
                    band_data[band][3] = int(round(row[1] * 60 / short_window))
            
            return band_data
                
        except Exception as e:
            self.logger.error(f"Error calculating band rates: {e}")
            self.logger.debug(traceback.format_exc())
            return {}

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None, rate_minutes=60):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_calculator = RateCalculator(self.db_path)
        self.setup_logging()

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
            
        except Exception as e:
            print(f"Error setting up logging: {e}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            raise
        
    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get base query results
                base_query = GET_STATION_DETAILS
                params = [contest, contest]
                
                # Add QTH filter if specified
                if filter_type and filter_value and filter_type.lower() != 'none':
                    filter_map = {
                        'DXCC': 'dxcc_country',
                        'CQ Zone': 'cq_zone',
                        'IARU Zone': 'iaru_zone',
                        'ARRL Section': 'arrl_section',
                        'State/Province': 'state_province',
                        'Continent': 'continent'
                    }
                    
                    if field := filter_map.get(filter_type):
                        base_query = base_query.replace(
                            "WHERE cs.contest = ?",
                            f"WHERE cs.contest = ? AND qi.{field} = ?"
                        )
                        params.append(filter_value)
    
                # Handle position filter
                position_filter = request.args.get('position_filter', 'all')
                if position_filter == 'range':
                    params.extend([callsign, callsign, callsign])
                else:
                    params.extend([callsign, callsign, callsign])  # Add third callsign param for consistency

                cursor.execute(base_query, params)
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
                params = (
                    callsign, contest, timestamp,                  # current_score parameters (3)
                    callsign, contest, timestamp, timestamp,       # long_window_score parameters (4)
                    callsign, contest, timestamp, timestamp        # short_window_score parameters (4)
                )
                
                cursor.execute(GET_BAND_BREAKDOWN_WITH_RATES, params)
                results = cursor.fetchall()
                band_data = {}
                
                for row in results:
                    band, current_qsos, multipliers, long_window_qsos, short_window_qsos = row
                    
                    # Calculate 60-minute rate
                    long_rate = 0
                    if long_window_qsos is not None:
                        qso_diff = current_qsos - long_window_qsos
                        if qso_diff > 0:
                            long_rate = int(round((qso_diff * 60) / 60))  # 60-minute rate
                    
                    # Calculate 15-minute rate
                    short_rate = 0
                    if short_window_qsos is not None:
                        qso_diff = current_qsos - short_window_qsos
                        if qso_diff > 0:
                            short_rate = int(round((qso_diff * 60) / 15))  # Convert 15-minute to hourly rate
                    
                    band_data[band] = [current_qsos, multipliers, long_rate, short_rate]
                
                return band_data
                        
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

    # ... (rest of the ScoreReporter class remains unchanged)
