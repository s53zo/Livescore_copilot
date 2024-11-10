#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime
from flask import request
import sys

class RateCalculator:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def calculate_rates(self, cursor, callsign, contest, current_ts, long_window=60, short_window=15):
        """Calculate QSO rates for both long and short time windows"""
        query = """
            WITH current_score AS (
                SELECT qsos, timestamp
                FROM contest_scores
                WHERE callsign = ? 
                AND contest = ?
                AND timestamp = ?
            ),
            long_window_score AS (
                SELECT qsos
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= datetime(?, '-60 minutes')
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(datetime(?, '-60 minutes')))
                LIMIT 1
            ),
            short_window_score AS (
                SELECT qsos
                FROM contest_scores
                WHERE callsign = ?
                AND contest = ?
                AND timestamp <= datetime(?, '-15 minutes')
                ORDER BY ABS(JULIANDAY(timestamp) - JULIANDAY(datetime(?, '-15 minutes')))
                LIMIT 1
            )
            SELECT 
                cs.qsos as current_qsos,
                lws.qsos as long_window_qsos,
                sws.qsos as short_window_qsos
            FROM current_score cs
            LEFT JOIN long_window_score lws
            LEFT JOIN short_window_score sws
        """
        
        cursor.execute(query, (
            callsign, contest, current_ts,
            callsign, contest, current_ts, current_ts,
            callsign, contest, current_ts, current_ts
        ))
        
        result = cursor.fetchone()
        if not result:
            return 0, 0
            
        current_qsos, long_window_qsos, short_window_qsos = result
        
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
                
        return long_rate, short_rate

    def calculate_band_rates(self, cursor, callsign, contest, current_ts, long_window=60, short_window=15):
        """Calculate per-band QSO rates for both time windows"""
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
                AND cs.timestamp = ?
            ),
            long_window_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as long_window_qsos
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= ?
                AND cs.timestamp >= datetime(?, ? || ' minutes')
                ORDER BY cs.timestamp DESC
            ),
            short_window_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as short_window_qsos
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= ?
                AND cs.timestamp >= datetime(?, ? || ' minutes')
                ORDER BY cs.timestamp DESC
            )
            SELECT 
                cb.band,
                cb.current_qsos,
                cb.multipliers,
                lwb.long_window_qsos,
                swb.short_window_qsos
            FROM current_bands cb
            LEFT JOIN long_window_bands lwb ON cb.band = lwb.band
            LEFT JOIN short_window_bands swb ON cb.band = swb.band
            WHERE cb.current_qsos > 0
            ORDER BY cb.band
        """
        
        cursor.execute(query, (
            callsign, contest, current_ts,
            callsign, contest, current_ts, current_ts, f"-{long_window}",
            callsign, contest, current_ts, current_ts, f"-{short_window}"
        ))
        
        results = cursor.fetchall()
        band_data = {}
        
        for row in results:
            band, current_qsos, multipliers, long_window_qsos, short_window_qsos = row
            
            # Calculate long window rate (60-minute)
            long_rate = 0
            if long_window_qsos is not None:
                qso_diff = current_qsos - long_window_qsos
                if qso_diff > 0:
                    long_rate = int(round((qso_diff * 60) / long_window))
            
            # Calculate short window rate (15-minute)
            short_rate = 0
            if short_window_qsos is not None:
                qso_diff = current_qsos - short_window_qsos
                if qso_diff > 0:
                    short_rate = int(round((qso_diff * 60) / short_window))
            
            band_data[band] = [current_qsos, multipliers, long_rate, short_rate]
        
        return band_data

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
            # Get filter information for the header if available
            filter_info_div = ""
            current_filter_type = request.args.get('filter_type', 'none')
            current_filter_value = request.args.get('filter_value', 'none')
    
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
                            if current_filter_type == label and current_filter_value == value:
                                filter_parts.append(
                                    f'<span class="active-filter">{label}: {value}</span>'
                                )
                            else:
                                filter_parts.append(
                                    f'<a href="/reports/live.html?contest={contest}'
                                    f'&callsign={callsign}&filter_type={label}'
                                    f'&filter_value={value}" class="filter-link">'
                                    f'{label}: {value}</a>'
                                )
                    
                    if filter_parts:
                        if current_filter_type != 'none':
                            filter_parts.append(
                                f'<a href="/reports/live.html?contest={contest}'
                                f'&callsign={callsign}&filter_type=none'
                                f'&filter_value=none" class="filter-link clear-filter">'
                                f'Show All</a>'
                            )
                        
                        filter_info_div = f"""
                        <div class="filter-info">
                            <span class="filter-label">Filters:</span> 
                            {' | '.join(filter_parts)}
                        </div>
                        """
    
            # Add category-specific CSS
            additional_css = """
                <style>
                    .category-group {
                        display: inline-flex;
                        gap: 4px;
                        font-size: 0.75rem;
                        line-height: 1;
                        align-items: center;
                    }
                    
                    .category-tag {
                        display: inline-block;
                        padding: 3px 6px;
                        border-radius: 3px;
                        white-space: nowrap;
                        font-family: monospace;
                    }
                    
                    /* Category colors */
                    .cat-power-high { background: #ffebee; color: #c62828; }
                    .cat-power-low { background: #e8f5e9; color: #2e7d32; }
                    .cat-power-qrp { background: #fff3e0; color: #ef6c00; }
                    
                    .cat-soa { background: #e3f2fd; color: #1565c0; }
                    .cat-so { background: #f3e5f5; color: #6a1b9a; }
                    .cat-ms { background: #fff8e1; color: #ff8f00; }
                    .cat-mm { background: #f1f8e9; color: #558b2f; }
                </style>
            """
    
            table_rows = []
            for i, station in enumerate(stations, 1):
                station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rn = station
                
                # Get additional category information from database
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
    
                # Calculate operator category
                op_category = self.get_operator_category(ops or 'SINGLE-OP', 
                                                       transmitter or 'ONE', 
                                                       assisted or 'NON-ASSISTED')
                
                # Format power class tag
                power_class = power.upper() if power else 'Unknown'
                display_power = 'H' if power_class == 'HIGH' else 'L' if power_class == 'LOW' else 'Q' if power_class == 'QRP' else 'U'
                power_tag = f'<span class="category-tag cat-power-{power_class.lower()}">{display_power}</span>' 
                
                # Create category display
                category_html = f"""
                    <div class="category-group">
                        <span class="category-tag cat-{op_category.lower().replace('/', '')}">{op_category}</span>
                        {power_tag}
                    </div>
                """
                
                # Get band breakdown with rates
                band_breakdown = self.get_band_breakdown_with_rates(
                    station_id, callsign_val, contest, timestamp
                )
                
                # Get reference station for rate comparison
                reference_station = next((s for s in stations if s[1] == callsign), None)
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
                
                # Add highlight for current station
                highlight = ' class="highlight"' if callsign_val == callsign else ''
                
                # Generate table row
                row = f"""
                <tr{highlight}>
                    <td>{i}</td>
                    <td>{callsign_val}</td>
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
                
            # Format final HTML
            html_content = template.format(
                contest=contest,
                callsign=callsign,
                timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                power=stations[0][3],
                assisted=stations[0][4],
                filter_info_div=filter_info_div,
                table_rows='\n'.join(table_rows),
                additional_css=additional_css
            )
            
            return html_content
    
        except Exception as e:
            self.logger.error(f"Error generating HTML content: {e}")
            self.logger.error(traceback.format_exc())
            raise
