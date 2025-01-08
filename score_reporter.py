#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime, timedelta
from flask import request
import sys
from sql_queries import (CALCULATE_RATES, CALCULATE_BAND_RATES,
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
        
    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None, position_filter=None):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Start building query
                base_query = """
                    WITH ranked_stations AS (
                        SELECT 
                            cs.id,
                            cs.callsign,
                            cs.score,
                            cs.power,
                            cs.assisted,
                            cs.timestamp,
                            cs.qsos,
                            cs.multipliers,
                            ROW_NUMBER() OVER (ORDER BY cs.score DESC) as position
                        FROM contest_scores cs
                        JOIN qth_info qi ON qi.contest_score_id = cs.id
                        WHERE cs.contest = ?
                        AND cs.id IN (
                            SELECT MAX(id)
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        )
                """
                
                params = [contest, contest]
                
                # Add QTH filter if specified
                if filter_type and filter_value and filter_type.lower() != 'none':
                    filter_map = {
                        'DXCC': 'qi.dxcc_country',
                        'CQ Zone': 'qi.cq_zone',
                        'IARU Zone': 'qi.iaru_zone',
                        'ARRL Section': 'qi.arrl_section',
                        'State/Province': 'qi.state_province',
                        'Continent': 'qi.continent'
                    }
                    
                    if field := filter_map.get(filter_type):
                        base_query += f" AND {field} = ?"
                        params.append(filter_value)
                
                base_query += ")"  # Close the CTE
                
                # Handle position filtering
                position_filter = position_filter or 'all'  # Default to 'all' if None
                if position_filter == 'range':
                    query = base_query + """
                        SELECT rs.*, 
                            CASE WHEN rs.callsign = ? THEN 'current'
                                    WHEN rs.score > (SELECT score FROM ranked_stations WHERE callsign = ?) 
                                    THEN 'above' ELSE 'below' END as rel_pos
                        FROM ranked_stations rs
                        WHERE EXISTS (
                            SELECT 1 FROM ranked_stations ref 
                            WHERE ref.callsign = ? 
                            AND ABS(rs.position - ref.position) <= 5
                        )
                        ORDER BY rs.score DESC
                    """
                    params.extend([callsign, callsign, callsign])
                else:
                    query = base_query + """
                        SELECT rs.*, 
                            CASE WHEN rs.callsign = ? THEN 'current'
                                    WHEN rs.score > (SELECT score FROM ranked_stations WHERE callsign = ?) 
                                    THEN 'above' ELSE 'below' END as rel_pos
                        FROM ranked_stations rs
                        ORDER BY rs.score DESC
                    """
                    params.extend([callsign, callsign])  # Only need two parameters here

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

    def generate_html_content(self, template, callsign, contest, stations, filter_type=None, filter_value=None, position_filter=None):
        """Generate HTML content from template and data"""
        try:
            from html import escape
            import re
            
            # Escape all data values
            safe_callsign = escape(callsign)
            safe_contest = escape(contest)
            
            # Build table rows with proper formatting
            table_rows = []
            reference_station = next((s for s in stations if s[1] == callsign), None)
            reference_timestamp = reference_station[5] if reference_station else None
            
            if reference_station:
                reference_band_data = self.get_band_breakdown_with_rates(
                    reference_station[0], callsign, contest, reference_station[5]
                )

            for i, station in enumerate(stations, 1):
                station_id, station_callsign, score, power, assisted, timestamp, qsos, mults, position, rel_pos = station
                
                # Get band breakdown for this station
                band_data = self.get_band_breakdown_with_rates(
                    station_id, station_callsign, contest, timestamp
                )
                
                # Get operator category
                operator_category = self.get_operator_category(
                    'SINGLE-OP' if assisted != 'MULTI-OP' else 'MULTI-OP',
                    'ONE',  # Default to ONE if not available
                    assisted
                )
                
                # Format each cell
                category_html = f"""
                    <div class="category-group">
                        <span class="category-tag cat-{operator_category.lower().replace('/', '')}">{operator_category}</span>
                        <span class="category-tag cat-power-{power.lower()}">{power[0]}</span>
                    </div>
                """
                
                # Format bands data
                bands = ['160', '80', '40', '20', '15', '10']
                band_cells = []
                for band in bands:
                    if band in band_data:
                        band_cells.append(self.format_band_data(band_data[band], reference_band_data if reference_station else None, band))
                    else:
                        band_cells.append("-/- (0/0)")
                
                # Get total rates
                long_rate, short_rate = self.get_total_rates(station_id, station_callsign, contest, timestamp)
                
                # Format timestamp
                ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
                
                # Build the row
                highlight = ' class="highlight"' if station_callsign == callsign else ''
                row = f"""
                <tr{highlight}>
                    <td>{i}</td>
                    <td><a href="/reports/live.html?contest={contest}&callsign={station_callsign}&filter_type={filter_type or 'none'}&filter_value={filter_value or 'none'}" style="color: inherit; text-decoration: none;">{station_callsign}</a></td>
                    <td>{category_html}</td>
                    <td>{score:,}</td>
                    <td class="band-data">{band_cells[0]}</td>
                    <td class="band-data">{band_cells[1]}</td>
                    <td class="band-data">{band_cells[2]}</td>
                    <td class="band-data">{band_cells[3]}</td>
                    <td class="band-data">{band_cells[4]}</td>
                    <td class="band-data">{band_cells[5]}</td>
                    <td class="band-data">{self.format_total_data(qsos, mults, long_rate, short_rate)}</td>
                    <td><span class="relative-time" data-timestamp="{timestamp}">{ts}</span></td>
                </tr>"""
                table_rows.append(row)
            
            # Create template variables
            template_vars = {
                'callsign': safe_callsign,
                'contest': safe_contest,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'power': reference_station[3] if reference_station else 'Unknown',
                'assisted': reference_station[4] if reference_station else 'Unknown',
                'table_rows': '\n'.join(table_rows),
                'filter_info_div': '<div class="filter-info">No filters applied</div>',
                'additional_css': ''
            }
            
            # Update JavaScript countdown numbers
            template = template.replace("${diff}", "${diff}")  # Fix JavaScript template literals
            template = template.replace("${{Math.floor(diff/60)}}", "${Math.floor(diff/60)}")
            template = template.replace("${{diff%60}}", "${diff%60}")
            template = template.replace("${{pad(seconds)}}", "${pad(seconds)}")
            
            # Perform template substitution
            def replace_var(match):
                var_name = match.group(1)
                return str(template_vars.get(var_name, ''))
            
            html_content = re.sub(r'\{(\w+)\}', replace_var, template)
            
            return html_content
            
        except Exception as e:
            self.logger.error(f"Error generating HTML content: {e}")
            self.logger.error(traceback.format_exc())
            return "<h1>Error generating report</h1>"

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

    def get_band_rates_from_table(self, cursor, callsign, contest, timestamp):
        """Calculate average of top 10 rates for a band"""
        # Get all non-zero 15-minute rates
        rates = []
        for band_data in self.get_band_breakdown_with_rates(None, callsign, contest, timestamp).values():
            if band_data[3] > 0:  # If there is a non-zero 15-minute rate
                rates.append(band_data[3])
        
        # Sort and take top 10
        top_rates = sorted(rates, reverse=True)[:10]
        return round(sum(top_rates) / len(top_rates)) if top_rates else 0

    def format_band_rates(self, rate):
        """Format average rate for display in header"""
        if rate > 0:
            return f'<div class="band-rates">Top 10 avg: {rate}/h</div>'
        return ""
