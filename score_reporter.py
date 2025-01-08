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
            
            # Initialize filter_info_div with a default value
            filter_info_div = '<div class="filter-info">No filters applied</div>'
            
            # Escape data values for safety
            safe_callsign = escape(callsign)
            safe_contest = escape(contest)
            
            # Get QTH info and build filter info div
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT qi.dxcc_country, qi.cq_zone, qi.iaru_zone, 
                        qi.arrl_section, qi.state_province, qi.continent
                    FROM contest_scores cs
                    JOIN qth_info qi ON qi.contest_score_id = cs.id
                    WHERE cs.callsign = ? AND cs.contest = ?
                    ORDER BY cs.timestamp DESC
                    LIMIT 1
                """, (callsign, contest))
                qth_info = cursor.fetchone()
                
                if qth_info:
                    filter_labels = ["DXCC", "CQ Zone", "IARU Zone", "ARRL Section", 
                                "State/Province", "Continent"]
                    filter_parts = []
                    
                    for label, value in zip(filter_labels, qth_info):
                        if value:
                            if filter_type == label and filter_value == value:
                                filter_parts.append(
                                    f'<span class="active-filter">{label}: {value}</span>'
                                )
                            else:
                                filter_parts.append(
                                    f'<a href="/reports/live.html?contest={contest}'
                                    f'&callsign={callsign}&filter_type={label}'
                                    f'&filter_value={value}&position_filter={position_filter}" '
                                    f'class="filter-link">{label}: {value}</a>'
                                )

                    if filter_parts:
                        if filter_type != 'none':
                            filter_parts.append(
                                f'<a href="/reports/live.html?contest={contest}'
                                f'&callsign={callsign}&filter_type=none'
                                f'&filter_value=none&position_filter={position_filter}" '
                                f'class="filter-link clear-filter">Show All</a>'
                            )

                        position_toggle_url = f"/reports/live.html?contest={contest}&callsign={callsign}&filter_type={filter_type}&filter_value={filter_value}"
                        position_toggle = f"""
                        <a href="{position_toggle_url}&position_filter={'range' if position_filter == 'all' else 'all'}" 
                        class="filter-link {' active-filter' if position_filter == 'range' else ''}">
                        Only ±5 Positions
                        </a>
                        """

                        filter_info_div = f"""
                        <div class="filter-info">
                            <span class="filter-label">Filters:</span> 
                            {' | '.join(filter_parts)} | {position_toggle}
                        </div>
                        """

            # Calculate active operators per band
            active_ops = {'160': 0, '80': 0, '40': 0, '20': 0, '15': 0, '10': 0}
            for station in stations:
                station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rel_pos = station
                band_breakdown = self.get_band_breakdown_with_rates(
                    station_id, callsign_val, contest, timestamp
                )
                for band, data in band_breakdown.items():
                    if data[3] > 0:  # Check 15-minute rate
                        active_ops[band] += 1

            # Process each station for the table
            table_rows = []
            reference_station = next((s for s in stations if s[1] == callsign), None)

            for i, station in enumerate(stations, 1):
                station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rel_pos = station

                # Get operator category
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT ops, transmitter
                        FROM contest_scores
                        WHERE id = ?
                    """, (station_id,))
                    result = cursor.fetchone()
                    ops = result[0] if result else None
                    transmitter = result[1] if result else None

                op_category = self.get_operator_category(ops or 'SINGLE-OP', 
                                                    transmitter or 'ONE', 
                                                    assisted or 'NON-ASSISTED')

                # Format power display
                power_class = power.upper() if power else 'Unknown'
                display_power = 'H' if power_class == 'HIGH' else 'L' if power_class == 'LOW' else 'Q' if power_class == 'QRP' else 'U'
                power_tag = f'<span class="category-tag cat-power-{power_class.lower()}">{display_power}</span>'

                # Build category HTML
                category_html = f"""
                    <div class="category-group">
                        <span class="category-tag cat-{op_category.lower().replace('/', '')}">{op_category}</span>
                        {power_tag}
                    </div>
                """

                # Get band breakdown and rates
                band_breakdown = self.get_band_breakdown_with_rates(
                    station_id, callsign_val, contest, timestamp
                )

                if reference_station:
                    reference_breakdown = self.get_band_breakdown_with_rates(
                        reference_station[0], callsign, contest, reference_station[5]
                    )
                else:
                    reference_breakdown = {}

                # Calculate total rates
                total_long_rate, total_short_rate = self.get_total_rates(
                    station_id, callsign_val, contest, timestamp
                )

                # Format timestamp
                ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')

                # Build row HTML
                highlight = ' class="highlight"' if callsign_val == callsign else ''
                callsign_cell = f"""<td><a href="/reports/live.html?contest={contest.strip()}&callsign={callsign_val.strip()}&filter_type={filter_type or 'none'}&filter_value={filter_value or 'none'}&position_filter={position_filter}" style="color: inherit; text-decoration: none;">{callsign_val}</a></td>"""

                row = f"""
                <tr{highlight}>
                    <td>{i}</td>
                    {callsign_cell}
                    <td>{category_html}</td>
                    <td>{score:,}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('160'), reference_breakdown, '160')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('80'), reference_breakdown, '80')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('40'), reference_breakdown, '40')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('20'), reference_breakdown, '20')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('15'), reference_breakdown, '15')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('10'), reference_breakdown, '10')}</td>
                    <td class="band-data">{self.format_total_data(qsos, mults, total_long_rate, total_short_rate)}</td>
                    <td><span class="relative-time" data-timestamp="{timestamp}">{ts}</span></td>
                </tr>"""
                table_rows.append(row)

            # Calculate average rates for bands
            band_avg_rates = {}
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for band in ['160', '80', '40', '20', '15', '10']:
                    rates = []
                    for station in stations:
                        station_id = station[0]
                        station_timestamp = station[5]
                        breakdown = self.get_band_breakdown_with_rates(
                            station_id, station[1], contest, station_timestamp
                        )
                        if band in breakdown and breakdown[band][3] > 0:  # 15-minute rate
                            rates.append(breakdown[band][3])

                    if rates:
                        top_rates = sorted(rates, reverse=True)[:10]
                        avg_rate = round(sum(top_rates) / len(top_rates))
                        band_avg_rates[band] = self.format_band_rates(avg_rate)

            # Replace band headers in template
            html_content = template
            for band in ['160', '80', '40', '20', '15', '10']:
                count = active_ops[band]
                rates_html = band_avg_rates.get(band, "")
                html_content = html_content.replace(
                    f'>{band}m</th>',
                    f' class="band-header"><span class="band-rates">{count}OPs@</span> {band}m{rates_html}</th>'
                )

            # Final template variables
            template_vars = {
                'contest': safe_contest,
                'callsign': safe_callsign,
                'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'power': stations[0][3] if stations else 'Unknown',
                'assisted': stations[0][4] if stations else 'Unknown',
                'filter_info_div': filter_info_div,
                'table_rows': '\n'.join(table_rows),
                'additional_css': ''
            }

            # Perform template substitution
            def replace_var(match):
                var_name = match.group(1)
                return str(template_vars.get(var_name, ''))

            html_content = re.sub(r'\{(\w+)\}', replace_var, html_content)
            
            return html_content

        except Exception as e:
            self.logger.error(f"Error generating HTML content: {e}")
            self.logger.error(traceback.format_exc())
            raise

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
