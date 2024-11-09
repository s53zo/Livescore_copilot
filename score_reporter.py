#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime
from flask import request

class RateCalculator:
    def __init__(self, db_path):
        self.db_path = db_path
        
    def calculate_band_rates(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate per-band QSO rates"""
        query = """
            WITH latest_scores AS (
                SELECT cs.id, cs.timestamp
                FROM contest_scores cs
                WHERE cs.callsign = ? 
                AND cs.contest = ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            ),
            latest_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as current_qsos,
                    cs.timestamp as current_ts
                FROM latest_scores ls
                JOIN contest_scores cs ON cs.id = ls.id
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            ),
            previous_bands AS (
                SELECT 
                    bb.band,
                    bb.qsos as prev_qsos,
                    cs.timestamp as prev_ts
                FROM contest_scores cs
                JOIN band_breakdown bb ON bb.contest_score_id = cs.id
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= datetime('now', ? || ' minutes')
                ORDER BY cs.timestamp DESC
                LIMIT 1
            )
            SELECT 
                lb.band,
                lb.current_qsos,
                pb.prev_qsos,
                lb.current_ts,
                pb.prev_ts,
                ROUND((JULIANDAY(lb.current_ts) - JULIANDAY(pb.prev_ts)) * 24 * 60, 1) as minutes_diff
            FROM latest_bands lb
            LEFT JOIN previous_bands pb ON lb.band = pb.band
            WHERE lb.current_qsos > 0
        """
        
        minutes_param = f"-{lookback_minutes}"
        cursor.execute(query, (callsign, contest, callsign, contest, minutes_param))
        results = cursor.fetchall()
        
        band_rates = {}
        for row in results:
            band, current_qsos, prev_qsos, current_ts, prev_ts, minutes_diff = row
            
            if not prev_ts or not minutes_diff or minutes_diff <= 0:
                band_rates[band] = 0
                continue
                
            # If previous QSOs is NULL, treat as 0
            prev_qsos = prev_qsos or 0
            qso_diff = current_qsos - prev_qsos
            
            if qso_diff == 0:
                band_rates[band] = 0
            else:
                # Calculate hourly rate
                rate = int(round((qso_diff * 60) / minutes_diff))
                band_rates[band] = rate
                
        return band_rates
        
    def calculate_total_rate(self, cursor, callsign, contest, lookback_minutes=60):
        """Calculate total QSO rate across all bands"""
        query = """
            WITH current_score AS (
                SELECT 
                    cs.qsos as current_qsos,
                    cs.timestamp as current_ts
                FROM contest_scores cs
                WHERE cs.callsign = ?
                AND cs.contest = ?
                ORDER BY cs.timestamp DESC
                LIMIT 1
            ),
            previous_score AS (
                SELECT 
                    cs.qsos as prev_qsos,
                    cs.timestamp as prev_ts
                FROM contest_scores cs
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= datetime('now', ? || ' minutes')
                ORDER BY cs.timestamp DESC
                LIMIT 1
            )
            SELECT 
                current_qsos,
                prev_qsos,
                current_ts,
                prev_ts,
                ROUND((JULIANDAY(current_ts) - JULIANDAY(prev_ts)) * 24 * 60, 1) as minutes_diff
            FROM current_score, previous_score
        """
        
        minutes_param = f"-{lookback_minutes}"
        cursor.execute(query, (callsign, contest, callsign, contest, minutes_param))
        result = cursor.fetchone()
        
        if not result or None in result:
            return 0
            
        current_qsos, prev_qsos, current_ts, prev_ts, minutes_diff = result
        
        if not minutes_diff or minutes_diff <= 0:
            return 0
            
        qso_diff = current_qsos - prev_qsos
        
        if qso_diff == 0:
            return 0
            
        return int(round((qso_diff * 60) / minutes_diff))

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None, rate_minutes=60):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_minutes = rate_minutes
        self.setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}, Rate interval: {rate_minutes} minutes")

    def setup_logging(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger('ScoreReporter')
        if not self.logger.handlers:
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            )
            file_handler = logging.FileHandler('/opt/livescore/logs/score_reporter.log')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.DEBUG)

    def get_band_breakdown_with_rate(self, station_id, callsign, contest):
        """Get band breakdown and calculate QSO rate for each band"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get current band breakdown
                cursor.execute("""
                    WITH CurrentScore AS (
                        SELECT cs.id, cs.timestamp
                        FROM contest_scores cs
                        WHERE cs.callsign = ?
                        AND cs.contest = ?
                        ORDER BY cs.timestamp DESC
                        LIMIT 1
                    )
                    SELECT 
                        bb.band, 
                        bb.qsos as band_qsos, 
                        bb.multipliers
                    FROM band_breakdown bb
                    JOIN CurrentScore cs ON cs.id = bb.contest_score_id
                    WHERE bb.qsos > 0
                    ORDER BY bb.band
                """, (callsign, contest))
                current_data = cursor.fetchall()
                
                if not current_data:
                    return {}
                
                # Calculate rates using the RateCalculator
                rate_calc = RateCalculator(self.db_path)
                band_rates = rate_calc.calculate_band_rates(cursor, callsign, contest, self.rate_minutes)
                
                # Combine current data with calculated rates
                result = {}
                for band, qsos, mults in current_data:
                    result[band] = [qsos, mults, band_rates.get(band, 0)]
                
                return result
                    
        except Exception as e:
            self.logger.error(f"Error in band rate calculation: {e}")
            self.logger.error(traceback.format_exc())
            return {}

    def get_total_qso_rate(self, station_id, callsign, contest):
        """Calculate total QSO rate over specified time period"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                rate_calc = RateCalculator(self.db_path)
                return rate_calc.calculate_total_rate(cursor, callsign, contest, self.rate_minutes)
        except Exception as e:
            self.logger.error(f"Error in total rate calculation: {e}")
            self.logger.error(traceback.format_exc())
            return 0

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
                            qi.state_province
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
                        'State/Province': 'state_province'
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
                
                # Add callsign parameters for the CASE statement
                params.extend([callsign, callsign])
                
                self.logger.debug(f"Executing query with params: {params}")
                cursor.execute(query, params)
                stations = cursor.fetchall()
                
                self.logger.debug(f"Query returned {len(stations)} stations")
                return stations
                    
        except Exception as e:
            self.logger.error(f"Error in get_station_details: {e}")
            self.logger.error(traceback.format_exc())
            return None

    def format_band_data(self, band_data):
        """Format band data as QSO/Mults (rate/h)"""
        if band_data:
            qsos, mults, rate = band_data
            if qsos > 0:
                rate_str = f"{rate:+d}" if rate != 0 else "0"
                return f"{qsos}/{mults} ({rate_str})"
        return "-/- (0)"

    def format_total_data(self, qsos, mults, rate):
        """Format total QSO/Mults with rate"""
        rate_str = f"{rate:+d}" if rate != 0 else "0"
        return f"{qsos}/{mults} ({rate_str})"

    def generate_html_content(self, template, callsign, contest, stations):
        """Generate HTML content directly without writing to file"""
        try:
            # Get filter information for the header if available
            filter_info = ""
            filter_info_div = ""
            current_filter_type = request.args.get('filter_type', 'none')
            current_filter_value = request.args.get('filter_value', 'none')

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT qi.dxcc_country, qi.cq_zone, qi.iaru_zone, 
                           qi.arrl_section, qi.state_province
                    FROM contest_scores cs
                    JOIN qth_info qi ON qi.contest_score_id = cs.id
                    WHERE cs.callsign = ? AND cs.contest = ?
                    ORDER BY cs.timestamp DESC
                    LIMIT 1
                """, (callsign, contest))
                qth_info = cursor.fetchone()
                
                if qth_info:
                    filter_labels = ["DXCC", "CQ Zone", "IARU Zone", "ARRL Section", "State/Province"]
                    filter_parts = []
                    
                    for label, value in zip(filter_labels, qth_info):
                        if value:
                            if current_filter_type == label and current_filter_value == value:
                                # Currently active filter
                                filter_parts.append(
                                    f'<span class="active-filter">{label}: {value}</span>'
                                )
                            else:
                                # Available filter, make it a link
                                filter_parts.append(
                                    f'<a href="/reports/live.html?contest={contest}'
                                    f'&callsign={callsign}&filter_type={label}'
                                    f'&filter_value={value}" class="filter-link">'
                                    f'{label}: {value}</a>'
                                )
                    
                    if filter_parts:
                        if current_filter_type != 'none':
                            # Add "Show All" link when a filter is active
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

            # Generate table rows
            table_rows = []
            for i, station in enumerate(stations, 1):
                station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rn = station
                
                # Get band breakdown and calculate rates for this station
                band_breakdown = self.get_band_breakdown_with_rate(station_id, callsign_val, contest)
                
                # Calculate total QSO rate for this station
                total_rate = self.get_total_qso_rate(station_id, callsign_val, contest)
                
                # Format timestamp for display
                ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
                
                # Add highlight class for the current station being monitored
                highlight = ' class="highlight"' if callsign_val == callsign else ''
                
                # Create the HTML table row with all data
                row = f"""
                <tr{highlight}>
                    <td>{i}</td>
                    <td>{callsign_val}</td>
                    <td>{score:,}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('160'))}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('80'))}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('40'))}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('20'))}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('15'))}</td>
                    <td class="band-data">{self.format_band_data(band_breakdown.get('10'))}</td>
                    <td class="band-data">{self.format_total_data(qsos, mults, total_rate)}</td>
                    <td>
                        <span class="relative-time" 
                              data-timestamp="{timestamp}">
                            {ts}
                        </span>
                    </td>
                </tr>"""
                table_rows.append(row)
            
            # Add additional CSS for filters
            additional_css = """
            <style>
            .filter-info {
                margin-top: 10px;
                padding: 8px;
                background-color: #f8f9fa;
                border-radius: 4px;
            }
            .filter-label {
                font-weight: bold;
                color: #666;
            }
            .filter-link {
                color: #0066cc;
                text-decoration: none;
                padding: 2px 6px;
                border-radius: 3px;
            }
            .filter-link:hover {
                background-color: #e7f3ff;
                text-decoration: underline;
            }
            .active-filter {
                background-color: #4CAF50;
                color: white;
                padding: 2px 6px;
                border-radius: 3px;
                font-weight: bold;
            }
            .clear-filter {
                margin-left: 10px;
                color: #666;
                border: 1px solid #ddd;
            }
            .clear-filter:hover {
                background-color: #f8f9fa;
                border-color: #666;
            }
            </style>
            """

            # Format HTML with all components
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
