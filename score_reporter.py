#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime, timedelta
from flask import request
import sys

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

    def calculate_band_rates(self, cursor, callsign, contest, timestamp, long_window=60, short_window=15):
        """Calculate per-band QSO rates for both time windows"""
        # Convert timestamp string to datetime if needed
        if isinstance(timestamp, str):
            current_ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        else:
            current_ts = timestamp
            
        # Calculate lookback times
        long_lookback = current_ts - timedelta(minutes=long_window)
        short_lookback = current_ts - timedelta(minutes=short_window)
        
        query = """
            WITH current_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as current_qsos,
                    bb.multipliers,
                    cs.timestamp as current_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ? 
                AND cs.contest = ?
                AND cs.timestamp <= ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            ),
            long_window_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as long_window_qsos,
                    cs.timestamp as long_window_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= ?
                ORDER BY ABS(JULIANDAY(cs.timestamp) - JULIANDAY(?))
                LIMIT 1
            ),
            short_window_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as short_window_qsos,
                    cs.timestamp as short_window_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= ?
                ORDER BY ABS(JULIANDAY(cs.timestamp) - JULIANDAY(?))
                LIMIT 1
            )
            SELECT 
                cb.band,
                cb.current_qsos,
                cb.multipliers,
                lwb.long_window_qsos,
                swb.short_window_qsos,
                cb.current_ts,
                lwb.long_window_ts,
                swb.short_window_ts
            FROM current_bands cb
            LEFT JOIN long_window_bands lwb ON cb.band = lwb.band
            LEFT JOIN short_window_bands swb ON cb.band = swb.band
            WHERE cb.current_qsos > 0
            ORDER BY cb.band
        """
        
        cursor.execute(query, (
            callsign, contest, current_ts.strftime('%Y-%m-%d %H:%M:%S'),
            callsign, contest, long_lookback.strftime('%Y-%m-%d %H:%M:%S'), long_lookback.strftime('%Y-%m-%d %H:%M:%S'),
            callsign, contest, short_lookback.strftime('%Y-%m-%d %H:%M:%S'), short_lookback.strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        results = cursor.fetchall()
        band_data = {}
        
        for row in results:
            band, current_qsos, multipliers, long_window_qsos, short_window_qsos, current_ts_str, long_window_ts, short_window_ts = row
            
            # Calculate 60-minute rate
            long_rate = 0
            if long_window_qsos is not None and long_window_ts:
                try:
                    time_diff = (datetime.strptime(current_ts_str, '%Y-%m-%d %H:%M:%S') - 
                               datetime.strptime(long_window_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60
                    if time_diff > 0:
                        qso_diff = current_qsos - long_window_qsos
                        if qso_diff > 0:
                            long_rate = int(round((qso_diff * 60) / time_diff))
                except Exception as e:
                    self.logger.error(f"Error calculating long rate: {e}")
                    long_rate = 0
            
            # Calculate 15-minute rate
            short_rate = 0
            if short_window_qsos is not None and short_window_ts:
                try:
                    time_diff = (datetime.strptime(current_ts_str, '%Y-%m-%d %H:%M:%S') - 
                               datetime.strptime(short_window_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60
                    if time_diff > 0:
                        qso_diff = current_qsos - short_window_qsos
                        if qso_diff > 0:
                            short_rate = int(round((qso_diff * 60) / time_diff))
                except Exception as e:
                    self.logger.error(f"Error calculating short rate: {e}")
                    short_rate = 0
            
            band_data[band] = [current_qsos, multipliers, long_rate, short_rate]
        
        return band_data

    def calculate_rates(self, cursor, callsign, contest, current_ts, long_window=60, short_window=15):
        """Calculate total QSO rates for both time windows using current UTC time"""
        long_window = int(long_window)
        short_window = int(short_window)
        
        current_utc = datetime.utcnow()
        long_lookback = current_utc - timedelta(minutes=long_window)
        short_lookback = current_utc - timedelta(minutes=short_window)
        
        if self.debug:
            self.logger.debug(f"\nCalculating total rates for {callsign} in {contest}")
            self.logger.debug(f"Current UTC: {current_utc}")
            self.logger.debug(f"Long window lookback to: {long_lookback}")
            self.logger.debug(f"Short window lookback to: {short_lookback}")
        
        query = """
            WITH current_score AS (
                SELECT qsos, timestamp
                FROM contest_scores
                WHERE callsign = ? 
                AND contest = ?
                AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            long_window_score AS (
                SELECT qsos, timestamp
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= ?
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(?))
                LIMIT 1
            ),
            short_window_score AS (
                SELECT qsos, timestamp
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= ?
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(?))
                LIMIT 1
            )
            SELECT 
                cs.qsos as current_qsos,
                lws.qsos as long_window_qsos,
                sws.qsos as short_window_qsos,
                cs.timestamp as current_ts,
                lws.timestamp as long_window_ts,
                sws.timestamp as short_window_ts
            FROM current_score cs
            LEFT JOIN long_window_score lws
            LEFT JOIN short_window_score sws
        """
        
        cursor.execute(query, (
            callsign, contest, current_utc.strftime('%Y-%m-%d %H:%M:%S'),
            callsign, contest, long_lookback.strftime('%Y-%m-%d %H:%M:%S'), long_lookback.strftime('%Y-%m-%d %H:%M:%S'),
            callsign, contest, short_lookback.strftime('%Y-%m-%d %H:%M:%S'), short_lookback.strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        result = cursor.fetchone()
        if not result:
            return 0, 0
            
        current_qsos, long_window_qsos, short_window_qsos, current_ts, long_window_ts, short_window_ts = result
        
        if self.debug:
            self.logger.debug("\nTotal rate analysis:")
            self.logger.debug(f"  Current QSOs: {current_qsos}")
            self.logger.debug(f"  Current timestamp: {current_ts}")
            self.logger.debug(f"  60-min window QSOs: {long_window_qsos} at {long_window_ts}")
            self.logger.debug(f"  15-min window QSOs: {short_window_qsos} at {short_window_ts}")
        
        # Calculate 60-minute rate
        long_rate = 0
        if long_window_qsos is not None and long_window_ts:
            time_diff = (datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S') - 
                        datetime.strptime(long_window_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60
            if time_diff > 0:
                qso_diff = current_qsos - long_window_qsos
                if qso_diff > 0:
                    long_rate = int(round((qso_diff * 60) / time_diff))
        
        # Calculate 15-minute rate
        short_rate = 0
        if short_window_qsos is not None and short_window_ts:
            time_diff = (datetime.strptime(current_ts, '%Y-%m-%d %H:%M:%S') - 
                        datetime.strptime(short_window_ts, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60
            if time_diff > 0:
                qso_diff = current_qsos - short_window_qsos
                if qso_diff > 0:
                    short_rate = int(round((qso_diff * 60) / time_diff))
        
        if self.debug:
            self.logger.debug(f"  60-minute rate: {long_rate}/hr")
            self.logger.debug(f"  15-minute rate: {short_rate}/hr")
            
        return long_rate, short_rate

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
            
            # Log startup message
            self.logger.info("Score Reporter logging initialized")
            self.logger.debug(f"Debug log file: {debug_log}")
            
        except Exception as e:
            print(f"Error setting up logging: {e}", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            raise
        
    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None):
        """Get station details with optimized query performance"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Build the optimized query
                query = """
                WITH ranked_scores AS (
                    SELECT 
                        cs.id,
                        cs.callsign,
                        cs.score,
                        cs.power,
                        cs.assisted,
                        cs.timestamp,
                        cs.qsos,
                        cs.multipliers,
                        ROW_NUMBER() OVER (
                            PARTITION BY cs.callsign 
                            ORDER BY cs.timestamp DESC
                        ) as rn
                    FROM contest_scores cs 
                    WHERE cs.contest = ?
                ),
                reference_score AS (
                    SELECT score
                    FROM ranked_scores
                    WHERE callsign = ?
                    AND rn = 1
                )
                SELECT 
                    rs.id,
                    rs.callsign,
                    rs.score,
                    rs.power,
                    rs.assisted,
                    rs.timestamp,
                    rs.qsos,
                    rs.multipliers,
                    CASE 
                        WHEN rs.callsign = ? THEN 'current'
                        WHEN rs.score > (SELECT score FROM reference_score) THEN 'above'
                        ELSE 'below'
                    END as position,
                    ROW_NUMBER() OVER (ORDER BY rs.score DESC) as rank
                FROM ranked_scores rs
                JOIN qth_info qi ON qi.contest_score_id = rs.id
                WHERE rs.rn = 1
                """
                
                params = [contest, callsign, callsign]

                # Add filter conditions if specified
                if filter_type and filter_value and filter_type.lower() != 'none':
                    filter_map = {
                        'DXCC': ('qi.dxcc_country', str),
                        'CQ Zone': ('qi.cq_zone', str),
                        'IARU Zone': ('qi.iaru_zone', str),
                        'ARRL Section': ('qi.arrl_section', str),
                        'State/Province': ('qi.state_province', str),
                        'Continent': ('qi.continent', str)
                    }
                    
                    if filter_type in filter_map:
                        field, value_type = filter_map[filter_type]
                        query += f" AND {field} = ?"
                        params.append(value_type(filter_value))

                # Complete the query
                query += " ORDER BY rs.score DESC"

                # Log query for debugging if needed
                self.logger.debug(f"Executing query with params: {params}")
                start_time = time.time()
                
                # Execute query
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                query_time = time.time() - start_time
                self.logger.debug(f"Query executed in {query_time:.3f} seconds")
                
                return results
                    
        except Exception as e:
            self.logger.error(f"Error in get_station_details: {e}")
            self.logger.error(traceback.format_exc())
            return None

    def get_band_breakdown_with_rates(self, station_id, callsign, contest, timestamp):
        """Get band breakdown with rates optimization"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = """
                WITH RECURSIVE time_windows(window_start) AS (
                    SELECT ?
                    UNION ALL
                    SELECT datetime(window_start, '-15 minutes')
                    FROM time_windows
                    WHERE window_start > datetime(?, '-60 minutes')
                )
                SELECT 
                    bb.band,
                    bb.qsos as current_qsos,
                    bb.multipliers,
                    prev_60.qsos as qsos_60,
                    prev_15.qsos as qsos_15
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                LEFT JOIN time_windows tw ON 1=1
                LEFT JOIN LATERAL (
                    SELECT bb2.qsos
                    FROM contest_scores cs2
                    JOIN band_breakdown bb2 ON bb2.contest_score_id = cs2.id
                    WHERE cs2.callsign = cs.callsign
                    AND cs2.contest = cs.contest
                    AND cs2.timestamp <= datetime(tw.window_start, '-60 minutes')
                    AND bb2.band = bb.band
                    ORDER BY cs2.timestamp DESC
                    LIMIT 1
                ) as prev_60
                LEFT JOIN LATERAL (
                    SELECT bb3.qsos
                    FROM contest_scores cs3
                    JOIN band_breakdown bb3 ON bb3.contest_score_id = cs3.id
                    WHERE cs3.callsign = cs.callsign
                    AND cs3.contest = cs.contest
                    AND cs3.timestamp <= datetime(tw.window_start, '-15 minutes')
                    AND bb3.band = bb.band
                    ORDER BY cs3.timestamp DESC
                    LIMIT 1
                ) as prev_15
                WHERE cs.id = ?
                GROUP BY bb.band
                """
                
                cursor.execute(query, (timestamp, timestamp, station_id))
                results = cursor.fetchall()
                
                band_data = {}
                for row in results:
                    band, qsos, mults, qsos_60, qsos_15 = row
                    
                    # Calculate rates
                    rate_60 = ((qsos - qsos_60) * 60 // 60) if qsos_60 is not None else 0
                    rate_15 = ((qsos - qsos_15) * 60 // 15) if qsos_15 is not None else 0
                    
                    band_data[band] = [qsos, mults, rate_60, rate_15]
                
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


    def format_band_data(self, band_data, reference_rates=None, band=None):
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
    
    def format_total_data(self, qsos, mults, long_rate, short_rate):
            """Format total QSO/Mults with both rates"""
            long_rate_str = f"+{long_rate}" if long_rate > 0 else "0"
            short_rate_str = f"+{short_rate}" if short_rate > 0 else "0"
            return f"{qsos}/{mults} ({long_rate_str}/{short_rate_str})"

    @staticmethod
    def get_operator_category(operator, transmitter, assisted):
        """Map operation categories based on defined rules"""
        # Handle empty/NULL assisted value - default to NON-ASSISTED
        assisted = assisted if assisted else 'NON-ASSISTED'
        
        category_map = {
            ('SINGLE-OP', 'ONE', 'ASSISTED'): 'SOA',
            ('SINGLE-OP', 'ONE', 'NON-ASSISTED'): 'SO',
            ('SINGLE-OP', 'TWO', 'ASSISTED'): 'SOA',
            ('SINGLE-OP', 'TWO', 'NON-ASSISTED'): 'SO',
            ('SINGLE-OP', 'UNLIMITED', 'ASSISTED'): 'SOA',
            ('SINGLE-OP', 'UNLIMITED', 'NON-ASSISTED'): 'SO',
            ('CHECKLOG', 'ONE', 'NON-ASSISTED'): 'SO',
            ('CHECKLOG', 'ONE', 'ASSISTED'): 'SOA',
            ('MULTI-OP', 'ONE', 'ASSISTED'): 'M/S',
            ('MULTI-OP', 'ONE', 'NON-ASSISTED'): 'M/S',
            ('MULTI-OP', 'TWO', 'ASSISTED'): 'M/S',
            ('MULTI-OP', 'TWO', 'NON-ASSISTED'): 'M/S',
            ('MULTI-OP', 'UNLIMITED', 'ASSISTED'): 'M/M',
            ('MULTI-OP', 'UNLIMITED', 'NON-ASSISTED'): 'M/M'
        }
        return category_map.get((operator, transmitter, assisted), 'Unknown')

    
    def generate_html_content(self, template, callsign, contest, stations):
        """
        Generate HTML content with simple template variable replacement.
        
        Args:
            template (str): HTML template string
            callsign (str): Station callsign
            contest (str): Contest name
            stations (list): List of station data (not used in template but kept for compatibility)
        
        Returns:
            str: Formatted HTML content
        """
        try:
            # Input validation
            if not all([template, callsign, contest]):
                raise ValueError("Missing required parameters")
    
            # Get filter information from request args with safe defaults
            filter_type = request.args.get('filter_type', 'none')
            filter_value = request.args.get('filter_value', 'none')
            
            # Create replacement dictionary
            replacements = {
                '{contest}': contest,
                '{callsign}': callsign,
                '{filter_type}': filter_type,
                '{filter_value}': filter_value,
                '{timestamp}': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                '{additional_css}': ''  # Keep this for backward compatibility
            }
            
            # Perform all replacements
            html_content = template
            for key, value in replacements.items():
                html_content = html_content.replace(key, str(value))
                
            self.logger.debug(f"Generated HTML content for {callsign} in {contest}")
            return html_content
            
        except ValueError as ve:
            self.logger.error(f"Invalid parameters in generate_html_content: {ve}")
            raise
        except Exception as e:
            self.logger.error(f"Error generating HTML content: {e}")
            self.logger.error(traceback.format_exc())
            raise
