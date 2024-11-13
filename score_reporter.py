#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime
from flask import request
import sys
import time

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
    def __init__(self, db_path=None, template_path=None):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_calculator = RateCalculator(self.db_path)  # Initialize the RateCalculator
        self.setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}")

    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None):
        """Get station details and all competitors in the same category with detailed logging"""
        try:
            # Use with statement for automatic connection management
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Log the initial query parameters
                self.logger.debug("Query parameters: contest=%s, callsign=%s, filter_type=%s, filter_value=%s",
                               contest, callsign, filter_type, filter_value)
                
                # First get the reference station's details
                cursor.execute("""
                    SELECT cs.id, cs.power, cs.assisted
                    FROM contest_scores cs
                    WHERE cs.contest = ? 
                    AND cs.callsign = ?
                    ORDER BY cs.timestamp DESC
                    LIMIT 1
                """, (contest, callsign))
                
                station_record = cursor.fetchone()
                if not station_record:
                    return None
                
                station_id, station_power, station_assisted = station_record
                self.logger.debug("Reference station - Power: %s, Assisted: %s", 
                               station_power, station_assisted)
                
                # Start building query
                query = """
                    WITH latest_scores AS (
                        SELECT callsign, MAX(timestamp) as max_ts
                        FROM contest_scores
                        WHERE contest = ?
                        GROUP BY callsign
                    ),
                    station_scores AS (
                        SELECT 
                            cs.id,
                            cs.callsign,
                            cs.score,
                            cs.power,
                            cs.assisted,
                            cs.timestamp,
                            cs.qsos,
                            cs.multipliers,
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
                        AND (cs.callsign = ? OR (cs.power = ? AND cs.assisted = ?))
                """
                
                params = [contest, contest, callsign, station_power, station_assisted]
                
                # Add filter condition if provided
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
                        self.logger.debug("Added filter condition: %s = %s", db_field, filter_value)
                else:
                    self.logger.debug("No filter condition added (none filter case)")

                query += """
                    )
                    SELECT 
                        id, callsign, score, power, assisted, timestamp, qsos, multipliers,
                        CASE 
                            WHEN callsign = ? THEN 'current'
                            WHEN score > (SELECT score FROM station_scores WHERE callsign = ?) THEN 'above'
                            ELSE 'below'
                        END as position,
                        ROW_NUMBER() OVER (ORDER BY score DESC) as rn
                    FROM station_scores
                    ORDER BY score DESC
                """
                
                params.extend([callsign, callsign])
                
                # Log execution plan
                self.logger.debug("Execution plan:")
                cursor.execute("EXPLAIN QUERY PLAN " + query, params)
                plan = cursor.fetchall()
                for step in plan:
                    self.logger.debug("- %s", step[3])
                
                # Execute query with timing
                import time
                start_time = time.time()
                cursor.execute(query, params)
                stations = cursor.fetchall()
                query_time = time.time() - start_time
                
                self.logger.debug("Query executed in %.3f seconds", query_time)
                self.logger.debug("Retrieved %d stations", len(stations))
                
                return stations
                
        except Exception as e:
            self.logger.error(f"Error in get_station_details: {e}")
            self.logger.error(traceback.format_exc())
            return None

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


    def generate_html(self, callsign, contest, stations, output_dir):
        """Generate HTML report and save it to the output directory"""
        if not stations:
            self.logger.error("No station data available")
            return False
    
        try:
            # Load template
            with open(self.template_path, 'r', encoding='utf-8') as f:
                template = f.read()
    
            # Generate HTML content
            html_content = self.generate_html_content(template, callsign, contest, stations)
    
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Write HTML file
            output_file = os.path.join(output_dir, 'live.html')
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
                
            self.logger.info(f"Report generated successfully: {output_file}")
            return True
    
        except Exception as e:
            self.logger.error(f"Error generating HTML report: {e}")
            self.logger.error(traceback.format_exc())
            return False
            
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

    
    def generate_filter_info(self, callsign, contest):
        """Generate the filter information div"""
        try:
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
            
            return filter_info_div
        except Exception as e:
            self.logger.error(f"Error generating filter info: {e}")
            return ""
    
    def generate_category_html(self, power, assisted):
        """Generate the HTML for the category display"""
        power_class = power.upper() if power else 'Unknown'
        display_power = 'H' if power_class == 'HIGH' else 'L' if power_class == 'LOW' else 'Q' if power_class == 'QRP' else 'U'
        
        category_html = f"""
            <div class="category-group">
                <span class="category-tag">{assisted or 'Unknown'}</span>
                <span class="category-tag cat-power-{power_class.lower()}">{display_power}</span>
            </div>
        """
        return category_html
    
    def get_additional_css(self):
        """Return the additional CSS needed for the report"""
        return """
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
    
    def generate_html_content(self, template, callsign, contest, stations):
        """Generate HTML content with optimized rate calculation"""
        try:
            start_time = time.time()
            self.logger.debug(f"Starting HTML generation for {len(stations)} stations")
            
            # Pre-calculate all rates in a single database connection
            station_rates = {}
            station_band_rates = {}
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                self.logger.debug("Batch calculating rates")
                batch_start = time.time()
                
                # Get all station IDs and timestamps
                station_data = [(s[0], s[1], s[5]) for s in stations]  # id, callsign, timestamp
                
                # Get all band breakdowns in a single query
                station_ids = [s[0] for s in station_data]
                placeholders = ','.join('?' * len(station_ids))
                
                # Batch fetch all band breakdowns
                cursor.execute(f"""
                    SELECT cs.id, bb.band, bb.qsos, bb.multipliers
                    FROM contest_scores cs
                    JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                    WHERE cs.id IN ({placeholders})
                    ORDER BY cs.id, bb.band
                """, station_ids)
                
                # Process band data
                for row in cursor.fetchall():
                    station_id = row[0]
                    if station_id not in station_band_rates:
                        station_band_rates[station_id] = {}
                    station_band_rates[station_id][row[1]] = [row[2], row[3], 0, 0]  # qsos, mults, long_rate, short_rate
                
                # Calculate rates for each station in bulk
                self.logger.debug("Calculating individual station rates")
                for station_id, station_call, timestamp in station_data:
                    # Calculate total rates
                    total_long_rate, total_short_rate = self.rate_calculator.calculate_rates(
                        cursor, station_call, contest, timestamp
                    )
                    station_rates[station_id] = (total_long_rate, total_short_rate)
                    
                    # Calculate band rates
                    band_data = self.rate_calculator.calculate_band_rates(
                        cursor, station_call, contest, timestamp
                    )
                    if station_id in station_band_rates:
                        for band, rates in band_data.items():
                            if band in station_band_rates[station_id]:
                                station_band_rates[station_id][band][2:] = rates[2:]  # Update long and short rates
                
                batch_time = time.time() - batch_start
                self.logger.debug(f"Batch rate calculations completed in {batch_time:.3f} seconds")
    
            # Get reference station for rate comparison
            reference_station = next((s for s in stations if s[1] == callsign), None)
            if reference_station:
                reference_breakdown = station_band_rates.get(reference_station[0], {})
            else:
                reference_breakdown = {}
    
            # Generate table rows
            self.logger.debug("Generating table rows")
            table_rows = []
            row_start = time.time()
            
            for i, station in enumerate(stations, 1):
                station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rn = station
                
                # Get cached rates
                total_long_rate, total_short_rate = station_rates.get(station_id, (0, 0))
                band_breakdown = station_band_rates.get(station_id, {})
                
                formatted_ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
                highlight = ' class="highlight"' if callsign_val == callsign else ''
                
                row = f"""
                <tr{highlight}>
                    <td>{i}</td>
                    <td>{callsign_val}</td>
                    <td>{self.generate_category_html(power, assisted)}</td>
                    <td>{score:,}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('160'), reference_breakdown, '160')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('80'), reference_breakdown, '80')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('40'), reference_breakdown, '40')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('20'), reference_breakdown, '20')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('15'), reference_breakdown, '15')}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('10'), reference_breakdown, '10')}</td>
                    <td class="band-data">{self.format_total_data(qsos, mults, total_long_rate, total_short_rate)}</td>
                    <td><span class="relative-time" data-timestamp="{timestamp}">{formatted_ts}</span></td>
                </tr>"""
                table_rows.append(row)
            
            row_time = time.time() - row_start
            self.logger.debug(f"Table row generation completed in {row_time:.3f} seconds")
    
            # Format final HTML
            self.logger.debug("Generating final HTML")
            html_content = template.format(
                contest=contest,
                callsign=callsign,
                timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                power=stations[0][3],
                assisted=stations[0][4],
                filter_info_div=self.generate_filter_info(callsign, contest),
                table_rows='\n'.join(table_rows),
                additional_css=self.get_additional_css()
            )
            
            total_time = time.time() - start_time
            self.logger.debug(f"HTML generation completed in {total_time:.3f} seconds")
            return html_content
            
        except Exception as e:
            self.logger.error(f"Error generating HTML content: {e}")
            self.logger.error(traceback.format_exc())
            raise
