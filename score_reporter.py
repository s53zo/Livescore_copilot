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
                query = """
                    WITH current_score AS (
                        SELECT cs.id, cs.timestamp, bb.band, bb.qsos, bb.multipliers
                        FROM contest_scores cs
                        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                        WHERE cs.callsign = ? 
                        AND cs.contest = ?
                        AND cs.timestamp = ?
                    ),
                    long_window_score AS (
                        SELECT bb.band, bb.qsos
                        FROM contest_scores cs
                        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                        WHERE cs.callsign = ?
                        AND cs.contest = ?
                        AND cs.timestamp <= ?
                        AND cs.timestamp >= datetime(?, '-60 minutes')
                        ORDER BY cs.timestamp DESC
                    ),
                    short_window_score AS (
                        SELECT bb.band, bb.qsos
                        FROM contest_scores cs
                        JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                        WHERE cs.callsign = ?
                        AND cs.contest = ?
                        AND cs.timestamp <= ?
                        AND cs.timestamp >= datetime(?, '-15 minutes')
                        ORDER BY cs.timestamp DESC
                    )
                    SELECT 
                        cs.band,
                        cs.qsos as current_qsos,
                        cs.multipliers,
                        lws.qsos as long_window_qsos,
                        sws.qsos as short_window_qsos
                    FROM current_score cs
                    LEFT JOIN long_window_score lws ON cs.band = lws.band
                    LEFT JOIN short_window_score sws ON cs.band = sws.band
                    WHERE cs.qsos > 0
                    ORDER BY cs.band
                """
    
                cursor.execute(query, (
                    callsign, contest, timestamp,              # current_score parameters (3)
                    callsign, contest, timestamp, timestamp,   # long_window_score parameters (4)
                    callsign, contest, timestamp, timestamp    # short_window_score parameters (4)
                ))
                
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
        """Generate HTML content with updated category display"""
        try:
            # Get filter information from the URL parameters
            filter_type = request.args.get('filter_type', 'none')
            filter_value = request.args.get('filter_value', 'none')
            
            # Prepare filter info div
            filter_info_div = ""
            
            if filter_type != 'none' and filter_value != 'none':
                # Create "Show All" link
                clear_filter = f'<a href="/reports/live.html?contest={contest}&callsign={callsign}&filter_type=none&filter_value=none" class="filter-link">Show All</a>'
                
                # Create active filter display
                active_filter = f'<span class="active-filter">{filter_type}: {filter_value}</span>'
                
                filter_info_div = f"""
                <div class="filter-info">
                    <span class="filter-label">Active Filter:</span> 
                    {active_filter} | {clear_filter}
                </div>
                """
            
            # Get reference station for category display
            reference_station = next((s for s in stations if s[1] == callsign), None)
            if not reference_station:
                raise ValueError(f"Reference station {callsign} not found in results")
                
            # Format table rows
            table_rows = []
            for i, station in enumerate(stations, 1):
                station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rank = station
                
                # Get band breakdown with rates
                band_data = self.get_band_breakdown_with_rates(
                    station_id, callsign_val, contest, timestamp
                )
                
                # Calculate total rates
                total_long_rate, total_short_rate = self.get_total_rates(
                    station_id, callsign_val, contest, timestamp
                )
                
                # Format band cells
                band_cells = []
                for band in ['160', '80', '40', '20', '15', '10']:
                    band_data_formatted = self.format_band_data(
                        band_data.get(band, None),
                        reference_rates=band_data if callsign_val == callsign else None,
                        band=band
                    )
                    band_cells.append(f'<td class="band-data">{band_data_formatted}</td>')
                
                # Format the row
                highlight = ' class="highlight"' if callsign_val == callsign else ''
                timestamp_formatted = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
                
                row = f"""
                <tr{highlight}>
                    <td>{i}</td>
                    <td>{callsign_val}</td>
                    <td>{power if power else 'Unknown'}</td>
                    <td>{score:,}</td>
                    {''.join(band_cells)}
                    <td class="band-data">{self.format_total_data(qsos, mults, total_long_rate, total_short_rate)}</td>
                    <td><span class="relative-time" data-timestamp="{timestamp}">{timestamp_formatted}</span></td>
                </tr>"""
                table_rows.append(row)
            
            # Format final HTML
            formatted_html = template.format(
                contest=contest,
                callsign=callsign,
                power=reference_station[3] if reference_station else 'Unknown',
                assisted=reference_station[4] if reference_station else 'Unknown',
                timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                filter_info_div=filter_info_div,
                table_rows='\n'.join(table_rows),
                additional_css='',
                category_filter=filter_type,
                filter_type=filter_type,
                filter_value=filter_value
            )
            
            return formatted_html
            
        except Exception as e:
            self.logger.error(f"Error generating HTML content: {e}")
            self.logger.error(traceback.format_exc())
            raise
