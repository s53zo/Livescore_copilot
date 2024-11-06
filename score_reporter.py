#!/usr/bin/env python3
import sqlite3
import os
import logging
import traceback
from datetime import datetime

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

    def load_template(self):
        """Load HTML template from file"""
        try:
            self.logger.debug(f"Loading template from: {self.template_path}")
            with open(self.template_path, 'r') as f:
                return f.read()
        except IOError as e:
            self.logger.error(f"Error loading template: {e}")
            return None

    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None, category_filter='same'):
        """Get station details and nearby competitors with optional filtering"""
        self.logger.debug(f"get_station_details called with: callsign={callsign}, contest={contest}")
        self.logger.debug(f"Filters: type={filter_type}, value={filter_value}, category={category_filter}")

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # First verify the station exists
                cursor.execute("""
                    SELECT COUNT(*), MAX(timestamp)
                    FROM contest_scores
                    WHERE callsign = ? AND contest = ?
                """, (callsign, contest))
                count, last_update = cursor.fetchone()
                self.logger.debug(f"Initial check: Found {count} records, last update: {last_update}")
                
                if count == 0:
                    self.logger.error(f"No records found for {callsign} in {contest}")
                    return None

                # Base query structure
                query = """
                    WITH StationScore AS (
                        SELECT 
                            cs.id, 
                            cs.callsign, 
                            cs.score, 
                            cs.power, 
                            cs.assisted,
                            cs.timestamp, 
                            cs.qsos, 
                            cs.multipliers,
                            'current' as position,
                            1 as rn
                        FROM contest_scores cs
                        WHERE cs.callsign = ? 
                        AND cs.contest = ?
                        ORDER BY cs.timestamp DESC
                        LIMIT 1
                    ),
                    LatestScores AS (
                        SELECT cs.id, cs.callsign, cs.score, cs.power, cs.assisted,
                               cs.timestamp, cs.qsos, cs.multipliers,
                               qi.dxcc_country, qi.cq_zone, qi.iaru_zone
                        FROM contest_scores cs
                        JOIN (
                            SELECT callsign, MAX(timestamp) as max_ts
                            FROM contest_scores
                            WHERE contest = ?
                            GROUP BY callsign
                        ) latest ON cs.callsign = latest.callsign 
                            AND cs.timestamp = latest.max_ts
                        LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
                        WHERE cs.contest = ?
                        {filter_clause}
                    )
                    SELECT 
                        id,
                        callsign, 
                        score, 
                        power, 
                        assisted,
                        timestamp, 
                        qsos, 
                        multipliers,
                        position,
                        rn
                    FROM (
                        SELECT * FROM StationScore
                        UNION ALL
                        SELECT 
                            ls.id,
                            ls.callsign, 
                            ls.score, 
                            ls.power, 
                            ls.assisted,
                            ls.timestamp, 
                            ls.qsos, 
                            ls.multipliers,
                            CASE
                                WHEN ls.score > ss.score THEN 'above'
                                WHEN ls.score < ss.score THEN 'below'
                            END as position,
                            ROW_NUMBER() OVER (
                                PARTITION BY 
                                    CASE
                                        WHEN ls.score > ss.score THEN 'above'
                                        WHEN ls.score < ss.score THEN 'below'
                                    END
                                ORDER BY 
                                    CASE
                                        WHEN ls.score > ss.score THEN score END ASC,
                                    CASE
                                        WHEN ls.score < ss.score THEN score END DESC
                            ) as rn
                        FROM LatestScores ls
                        CROSS JOIN StationScore ss
                        WHERE ls.callsign != ss.callsign
                        {category_clause}
                    )
                    ORDER BY score DESC;
                """
                
                params = [callsign, contest, contest, contest]
                filter_clause = ""
                category_clause = ""
                
                # Add category filtering if requested
                if category_filter == 'same':
                    category_clause = """
                        AND ls.power = ss.power
                        AND ls.assisted = ss.assisted
                    """
                
                # Add location filtering if specified
                if filter_type and filter_value and str(filter_value).strip():
                    if filter_type == 'dxcc':
                        filter_clause = "AND qi.dxcc_country = ?"
                        params.append(filter_value.strip())
                    elif filter_type == 'cq_zone':
                        filter_clause = "AND CAST(qi.cq_zone AS TEXT) = ?"
                        params.append(str(filter_value).strip())
                    elif filter_type == 'iaru_zone':
                        filter_clause = "AND CAST(qi.iaru_zone AS TEXT) = ?"
                        params.append(str(filter_value).strip())
                
                # Format the query with the appropriate clauses
                formatted_query = query.format(
                    filter_clause=filter_clause,
                    category_clause=category_clause
                )
                
                self.logger.debug(f"Executing query with params: {params}")
                self.logger.debug(f"Category filter: {category_filter}")
                self.logger.debug(f"Category clause: {category_clause}")
                
                cursor.execute(formatted_query, params)
                stations = cursor.fetchall()
                
                if not stations:
                    self.logger.error("Main query returned no results")
                    self.logger.debug("Query used:")
                    self.logger.debug(formatted_query)
                    self.logger.debug("Parameters:")
                    self.logger.debug(params)
                    return None
                
                self.logger.debug(f"Found {len(stations)} stations")
                for station in stations:
                    self.logger.debug(f"Station: {station}")
                
                return stations
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            self.logger.error(traceback.format_exc())
            return None

    def get_total_qso_rate(self, station_id, callsign, contest):
        """Calculate total QSO rate over specified time period"""
        query = """
            WITH CurrentScore AS (
                SELECT cs.qsos
                FROM contest_scores cs
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= datetime('now')
                ORDER BY cs.timestamp DESC
                LIMIT 1
            ),
            PreviousScore AS (
                SELECT cs.qsos, cs.timestamp
                FROM contest_scores cs
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp <= datetime('now')
                ORDER BY ABS(JULIANDAY(cs.timestamp) - 
                          JULIANDAY(datetime('now', ? || ' minutes')))
                LIMIT 1
            )
            SELECT 
                (SELECT qsos FROM CurrentScore) as current_qsos,
                p.qsos as previous_qsos,
                (JULIANDAY(datetime('now')) - JULIANDAY(p.timestamp)) * 24 * 60 as minutes_diff
            FROM PreviousScore p
        """
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                minutes_param = f"-{self.rate_minutes}"
                cursor.execute(query, (callsign, contest, callsign, contest, minutes_param))
                result = cursor.fetchone()
                
                if result and None not in result and result[2] > 0:
                    current_qsos, previous_qsos, minutes_diff = result
                    qso_diff = current_qsos - previous_qsos
                    
                    if qso_diff == 0:
                        return 0
                        
                    rate = int(round((qso_diff * 60) / minutes_diff))
                    self.logger.debug(f"Total QSO rate for {callsign}: {rate}/hr "
                                    f"(+{qso_diff} QSOs in {minutes_diff:.1f} minutes)")
                    return rate
                return 0
        except sqlite3.Error as e:
            self.logger.error(f"Database error in total rate calculation: {e}")
            return 0
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            self.logger.error(traceback.format_exc())
            return 0

    def get_band_breakdown_with_rate(self, station_id, callsign, contest):
        """Get band breakdown and calculate QSO rate for each band"""
        current_query = """
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
        """
        
        previous_query = """
            WITH TimeTarget AS (
                SELECT datetime('now', ? || ' minutes') as target_time
            )
            SELECT 
                bb.band,
                bb.qsos,
                cs.timestamp
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            CROSS JOIN TimeTarget tt
            WHERE cs.callsign = ?
            AND cs.contest = ?
            AND cs.timestamp <= datetime('now')
            AND bb.qsos > 0
            ORDER BY ABS(JULIANDAY(cs.timestamp) - JULIANDAY(tt.target_time))
            LIMIT 1
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(current_query, (callsign, contest))
                current_data = cursor.fetchall()
                
                if not current_data:
                    return {}
                
                result = {}
                for row in current_data:
                    band, qsos, mults = row
                    result[band] = [qsos, mults, 0]
                
                minutes_param = f"-{self.rate_minutes}"
                cursor.execute(previous_query, (minutes_param, callsign, contest))
                previous_data = cursor.fetchall()
                
                if previous_data:
                    prev_timestamp = datetime.strptime(previous_data[0][2], '%Y-%m-%d %H:%M:%S')
                    minutes_diff = (datetime.utcnow() - prev_timestamp).total_seconds() / 60
    
                    prev_bands = {row[0]: row[1] for row in previous_data}
                    
                    for band in result:
                        current_qsos = result[band][0]
                        if band in prev_bands and minutes_diff > 0:
                            qso_diff = current_qsos - prev_bands[band]
                            
                            if qso_diff == 0:
                                continue
                                
                            rate = int(round((qso_diff * 60) / minutes_diff))
                            result[band][2] = rate
                            
                            self.logger.debug(f"Band {band} rate calculation:")
                            self.logger.debug(f"  Current QSOs: {current_qsos}")
                            self.logger.debug(f"  Previous QSOs: {prev_bands[band]}")
                            self.logger.debug(f"  Time span: {minutes_diff:.1f} minutes")
                            self.logger.debug(f"  Calculated rate: {rate}/hr")
                
                return result
                    
        except sqlite3.Error as e:
            self.logger.error(f"Database error in band rate calculation: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            self.logger.error(traceback.format_exc())
            return {}

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

    def generate_html(self, callsign, contest, stations, output_dir, filter_type=None, filter_value=None, category_filter='same'):
        """Generate HTML report"""
        if not stations:
            self.logger.error("No station data available")
            return False
    
        template = self.load_template()
        if not template:
            return False
    
        # Generate table rows
        table_rows = []
        for i, station in enumerate(stations, 1):
            station_id, callsign_val, score, power, assisted, timestamp, qsos, mults, position, rn = station
            
            # Get band breakdown for this station
            band_breakdown = self.get_band_breakdown_with_rate(station_id, callsign_val, contest)
            
            # Get total QSO rate
            total_rate = self.get_total_qso_rate(station_id, callsign_val, contest)
            
            # Format timestamp
            ts = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M')
            
            # Determine if this is the highlighted row
            highlight = ' class="highlight"' if callsign_val == callsign else ''
            
            # Create the table row
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
                <td>{ts}</td>
            </tr>"""
            table_rows.append(row)
    
        # Prepare filter display text
        if filter_type and filter_value:
            filter_display = f"| Filtered by: {filter_type.upper()}: {filter_value}"
        else:
            filter_display = ""
    
        # Category filter information
        category_checked = 'checked="checked"' if category_filter == 'all' else ''
        category_label = "Showing all categories" if category_filter == 'all' else "Showing only matching category"
    
        # Format HTML
        html_content = template.format(
            contest=contest,
            callsign=callsign,
            timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            power=stations[0][3],
            assisted=stations[0][4],
            table_rows='\n'.join(table_rows),
            filter_type=filter_type or '',
            filter_value=filter_value or '',
            filter_display=filter_display,
            category_filter=category_filter,
            category_checked=category_checked,
            category_label=category_label
        )
    
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Write HTML file
        output_file = os.path.join(output_dir, 'live.html')
        try:
            with open(output_file, 'w') as f:
                f.write(html_content)
            self.logger.info(f"Report generated: {output_file}")
            return True
        except IOError as e:
            self.logger.error(f"Error writing report: {e}")
            return False
