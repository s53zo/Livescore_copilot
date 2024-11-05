#!/usr/bin/env python3
import sqlite3
import os
import logging
from datetime import datetime

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None, rate_minutes=60):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.rate_minutes = rate_minutes
        self._setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}, Rate interval: {rate_minutes} minutes")

    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('ScoreReporter')

    def load_template(self):
        """Load HTML template from file"""
        try:
            self.logger.debug(f"Loading template from: {self.template_path}")
            with open(self.template_path, 'r') as f:
                return f.read()
        except IOError as e:
            self.logger.error(f"Error loading template: {e}")
            return None

    def get_station_details(self, callsign, contest, filter_type=None, filter_value=None):
        """
        Get station details and nearby competitors with optional filtering
        """
        self.logger.debug(f"Starting get_station_details with filter_type={filter_type}, filter_value={filter_value}")
    
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
                LEFT JOIN qth_info qi ON qi.contest_score_id = cs.id
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
                JOIN qth_info qi ON qi.contest_score_id = cs.id
                WHERE cs.contest = ?
                {filter_clause}
            ),
            ValidStations AS (
                SELECT 
                    ls.id,
                    ls.callsign, 
                    ls.score, 
                    ls.power, 
                    ls.assisted,
                    ls.timestamp, 
                    ls.qsos, 
                    ls.multipliers
                FROM LatestScores ls, StationScore ss
                WHERE ls.power = ss.power
                AND ls.assisted = ss.assisted
                AND ls.callsign != ss.callsign
            ),
            NearbyStations AS (
                SELECT 
                    vs.id,
                    vs.callsign, 
                    vs.score, 
                    vs.power, 
                    vs.assisted,
                    vs.timestamp, 
                    vs.qsos, 
                    vs.multipliers,
                    CASE
                        WHEN vs.score > (SELECT score FROM StationScore) THEN 'above'
                        WHEN vs.score < (SELECT score FROM StationScore) THEN 'below'
                    END as position,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            CASE
                                WHEN vs.score > (SELECT score FROM StationScore) THEN 'above'
                                WHEN vs.score < (SELECT score FROM StationScore) THEN 'below'
                            END
                        ORDER BY 
                            CASE
                                WHEN vs.score > (SELECT score FROM StationScore) THEN score END ASC,
                            CASE
                                WHEN vs.score < (SELECT score FROM StationScore) THEN score END DESC
                    ) as rn
                FROM ValidStations vs
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
                SELECT * FROM NearbyStations
                WHERE (position = 'above' AND rn <= 2)
                OR (position = 'below' AND rn <= 2)
            )
            ORDER BY score DESC;
        """
        
        params = [callsign, contest, contest, contest]
        filter_clause = ""
        
        if filter_type and filter_value:
            self.logger.debug(f"Applying filter: {filter_type}={filter_value}")
            if filter_type == 'dxcc':
                filter_clause = "AND qi.dxcc_country = ?"
                params.append(filter_value)
            elif filter_type == 'cq_zone':
                filter_clause = "AND CAST(qi.cq_zone AS TEXT) = ?"
                params.append(str(filter_value))
            elif filter_type == 'iaru_zone':
                filter_clause = "AND CAST(qi.iaru_zone AS TEXT) = ?"
                params.append(str(filter_value))
        
        formatted_query = query.format(filter_clause=filter_clause)
        self.logger.debug(f"Executing query with params: {params}")
        self.logger.debug(f"Filter clause: {filter_clause}")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Debug query to check QTH info
                if filter_type and filter_value:
                    debug_query = """
                        SELECT cs.callsign, qi.dxcc_country, qi.cq_zone, qi.iaru_zone
                        FROM contest_scores cs
                        JOIN qth_info qi ON qi.contest_score_id = cs.id
                        WHERE cs.contest = ?
                        ORDER BY cs.callsign
                    """
                    cursor.execute(debug_query, [contest])
                    debug_results = cursor.fetchall()
                    self.logger.debug(f"QTH info for contest: {debug_results}")
                
                cursor.execute(formatted_query, params)
                stations = cursor.fetchall()
                
                if not stations:
                    self.logger.error(f"No data found for {callsign} in {contest} with filter {filter_type}={filter_value}")
                    return None
                
                self.logger.debug(f"Found {len(stations)} matching stations")
                return stations
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            self.logger.error(f"Query: {formatted_query}")
            self.logger.error(f"Parameters: {params}")
            return None

    def get_total_qso_rate(self, station_id, callsign, contest):
        """Calculate total QSO rate over specified time period"""
        query = """
            WITH CurrentScore AS (
                SELECT cs.qsos, cs.timestamp
                FROM contest_scores cs
                WHERE cs.id = ?
            ),
            PreviousScore AS (
                SELECT cs.qsos, cs.timestamp
                FROM contest_scores cs
                WHERE cs.callsign = ?
                AND cs.contest = ?
                AND cs.timestamp < (SELECT timestamp FROM CurrentScore)
                AND cs.timestamp >= datetime((SELECT timestamp FROM CurrentScore), ? || ' minutes')
                ORDER BY cs.timestamp DESC
                LIMIT 1
            )
            SELECT 
                c.qsos as current_qsos,
                p.qsos as previous_qsos,
                ROUND((JULIANDAY(c.timestamp) - JULIANDAY(p.timestamp)) * 24 * 60, 2) as minutes_diff
            FROM CurrentScore c
            LEFT JOIN PreviousScore p
            """
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                minutes_param = f"-{self.rate_minutes}"
                cursor.execute(query, (station_id, callsign, contest, minutes_param))
                result = cursor.fetchone()
                
                if result and result[0] is not None and result[1] is not None and result[2] > 0:
                    current_qsos, previous_qsos, minutes_diff = result
                    qso_diff = current_qsos - previous_qsos
                    # Convert to hourly rate
                    rate = int(round(qso_diff * (60 / minutes_diff)))
                    self.logger.debug(f"Total QSO rate for {callsign}: {rate}/hr "
                                    f"(+{qso_diff} QSOs in {minutes_diff:.1f} minutes)")
                    return rate
                return 0
        except sqlite3.Error as e:
            self.logger.error(f"Database error in total rate calculation: {e}")
            return 0

    def get_band_breakdown_with_rate(self, station_id, callsign, contest):
        """Get band breakdown and calculate QSO rate for each band with interpolation"""
        current_query = """
            SELECT 
                bb.band, 
                bb.qsos as band_qsos, 
                bb.multipliers, 
                cs.timestamp
            FROM band_breakdown bb
            JOIN contest_scores cs ON cs.id = bb.contest_score_id
            WHERE bb.contest_score_id = ?;
        """
        
        previous_query = """
            WITH CurrentTimestamp AS (
                SELECT timestamp 
                FROM contest_scores 
                WHERE id = ?
            )
            SELECT 
                bb.band, 
                bb.qsos as prev_band_qsos,
                cs.timestamp,
                ABS(ROUND((JULIANDAY(cs.timestamp) - 
                          JULIANDAY((SELECT timestamp FROM CurrentTimestamp))) * 24 * 60, 2)) as minutes_diff
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.callsign = ? 
            AND cs.contest = ?
            AND cs.timestamp < (SELECT timestamp FROM CurrentTimestamp)
            AND cs.timestamp >= datetime((SELECT timestamp FROM CurrentTimestamp), ? || ' minutes')
            ORDER BY cs.timestamp DESC, bb.band;
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get current band breakdown
                cursor.execute(current_query, (station_id,))
                current_data = cursor.fetchall()
                
                if not current_data:
                    return {}
                
                # Organize current data and store current timestamp
                current_by_band = {}
                for row in current_data:
                    band, qsos, mults, current_timestamp = row
                    current_by_band[band] = {
                        'qsos': qsos,
                        'mults': mults,
                        'timestamp': current_timestamp
                    }
                
                # Get previous data points using rate_minutes
                minutes_param = f"-{self.rate_minutes}"
                cursor.execute(previous_query, (station_id, callsign, contest, minutes_param))
                previous_data = cursor.fetchall()
                
                # Calculate rates for each band
                result = {}
                for band in current_by_band:
                    current = current_by_band[band]
                    current_qsos = current['qsos']
                    rate = 0
                    
                    for prev_row in previous_data:
                        prev_band, prev_qsos, prev_timestamp, minutes_diff = prev_row
                        if prev_band == band and minutes_diff > 0:
                            qso_diff = current_qsos - prev_qsos
                            # Convert to hourly rate
                            rate = int(round(qso_diff * (60 / minutes_diff)))
                            break
                    
                    result[band] = (current_qsos, current['mults'], rate)
                
                return result
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error in band rate calculation: {e}")
            return {}

    def format_band_data(self, band_data):
        """Format band data as QSO/Mults (rate/h)"""
        if band_data:
            qsos, mults, rate = band_data
            rate_str = f"{rate:+d}" if rate != 0 else "0"
            return f"{qsos}/{mults} ({rate_str})"
        return "-/- (0)"

    def format_total_data(self, qsos, mults, rate):
        """Format total QSO/Mults with rate"""
        rate_str = f"{rate:+d}" if rate != 0 else "0"
        return f"{qsos}/{mults} ({rate_str})"

    def generate_html(self, callsign, contest, stations, output_dir, filter_type=None, filter_value=None):
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
    
        # Format HTML
        html_content = template.format(
            contest=contest,
            callsign=callsign,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            power=stations[0][3],
            assisted=stations[0][4],
            table_rows='\n'.join(table_rows),
            filter_type=filter_type or '',
            filter_value=filter_value or '',
            filter_display=filter_display
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
          
