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

    def get_station_details(self, callsign, contest, dxcc_country=None, cq_zone=None, iaru_zone=None):
    """Get station details and nearby competitors, with optional filters for country and zones."""
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
            LEFT JOIN qth_info qi ON cs.id = qi.contest_score_id
            WHERE cs.callsign = ? 
            AND cs.contest = ?
            ORDER BY cs.timestamp DESC
            LIMIT 1
        ),
        NearbyStations AS (
            SELECT 
                cs.id,
                cs.callsign, 
                cs.score, 
                cs.power, 
                cs.assisted,
                cs.timestamp, 
                cs.qsos, 
                cs.multipliers,
                CASE
                    WHEN cs.score > (SELECT score FROM StationScore) THEN 'above'
                    WHEN cs.score < (SELECT score FROM StationScore) THEN 'below'
                END as position,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        CASE
                            WHEN cs.score > (SELECT score FROM StationScore) THEN 'above'
                            WHEN cs.score < (SELECT score FROM StationScore) THEN 'below'
                        END
                    ORDER BY 
                        CASE
                            WHEN cs.score > (SELECT score FROM StationScore) THEN score END ASC,
                        CASE
                            WHEN cs.score < (SELECT score FROM StationScore) THEN score END DESC
                ) as rn
            FROM contest_scores cs
            LEFT JOIN qth_info qi ON cs.id = qi.contest_score_id
            WHERE cs.contest = ?
            AND cs.power = (SELECT power FROM StationScore)
            AND cs.assisted = (SELECT assisted FROM StationScore)
            AND cs.callsign != (SELECT callsign FROM StationScore)
            AND cs.timestamp = (
                SELECT MAX(timestamp)
                FROM contest_scores cs2
                WHERE cs2.callsign = cs.callsign
                AND cs2.contest = cs.contest
            )
    """

    # Add filtering conditions for dxcc_country, cq_zone, and iaru_zone
    params = [callsign, contest, contest]
    if dxcc_country:
        query += " AND qi.dxcc_country = ?"
        params.append(dxcc_country)
    if cq_zone:
        query += " AND qi.cq_zone = ?"
        params.append(cq_zone)
    if iaru_zone:
        query += " AND qi.iaru_zone = ?"
        params.append(iaru_zone)

    # Close the NearbyStations CTE and the final SELECT
    query += """
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

    try:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        self.logger.error(f"Database error: {e}")
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

    def generate_html(self, callsign, contest, stations, output_dir):
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

        # Format HTML
        html_content = template.format(
            contest=contest,
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            power=stations[0][3],
            assisted=stations[0][4],
            table_rows='\n'.join(table_rows)
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
          
