#!/usr/bin/env python3
import argparse
import sqlite3
import os
import sys
from datetime import datetime
import logging

class ScoreReporter:
    def __init__(self, db_path=None, template_path=None):
        """Initialize the ScoreReporter class"""
        self.db_path = db_path or 'contest_data.db'
        self.template_path = template_path or 'templates/score_template.html'
        self.setup_logging()
        self.logger.debug(f"Initialized with DB: {self.db_path}, Template: {self.template_path}")

    def setup_logging(self):
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

    def calculate_rate(self, current_qsos, previous_qsos, time_diff_hours):
        """Calculate hourly rate based on QSO difference and time difference"""
        if time_diff_hours <= 0:
            return 0
        qso_diff = current_qsos - previous_qsos
        # Interpolate to get hourly rate
        return int(round(qso_diff * (1.0 / time_diff_hours)))

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
                          JULIANDAY((SELECT timestamp FROM CurrentTimestamp))) * 24, 2)) as hours_diff
            FROM contest_scores cs
            JOIN band_breakdown bb ON bb.contest_score_id = cs.id
            WHERE cs.callsign = ? 
            AND cs.contest = ?
            AND cs.timestamp < (SELECT timestamp FROM CurrentTimestamp)
            AND cs.timestamp >= datetime((SELECT timestamp FROM CurrentTimestamp), '-4 hour')  -- Limit to last 4 hours
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
                    self.logger.debug(f"Current data for {band}: QSOs={qsos}, Mults={mults}")
                
                # Get previous data points
                cursor.execute(previous_query, (station_id, callsign, contest))
                previous_data = cursor.fetchall()
                
                # Calculate rates for each band
                result = {}
                for band in current_by_band:
                    current = current_by_band[band]
                    current_qsos = current['qsos']
                    
                    # Find the closest previous data point for this band
                    previous_qsos = 0
                    time_diff = 1.0  # Default to 1 hour if no previous data
                    rate = 0
                    
                    # Log current QSOs
                    self.logger.debug(f"Processing {band} band:")
                    self.logger.debug(f"  Current QSOs: {current_qsos}")
                    
                    for prev_row in previous_data:
                        prev_band, prev_qsos, prev_timestamp, hours_diff = prev_row
                        if prev_band == band:
                            previous_qsos = prev_qsos
                            time_diff = hours_diff
                            
                            # Log previous data found
                            self.logger.debug(f"  Found previous data:")
                            self.logger.debug(f"    Previous QSOs: {prev_qsos}")
                            self.logger.debug(f"    Time diff: {hours_diff:.2f} hours")
                            
                            if hours_diff > 0:
                                qso_diff = current_qsos - prev_qsos
                                rate = int(round(qso_diff / hours_diff))
                                
                                self.logger.debug(f"    QSO diff: {qso_diff}")
                                self.logger.debug(f"    Calculated rate: {rate}/hr")
                            break
                    
                    result[band] = (current_qsos, current['mults'], rate)
                
                return result
                
        except sqlite3.Error as e:
            self.logger.error(f"Database error in rate calculation: {e}")
            self.logger.error(f"Current query: {current_query}")
            self.logger.error(f"Previous query: {previous_query}")
            return {}
    
    def format_band_data(self, band_data):
        """Format band data as QSO/Mults (rate/h)"""
        if band_data:
            qsos, mults, rate = band_data
            rate_str = f"{rate:+d}" if rate != 0 else "0"  # Show + sign for positive rates
            return f"{qsos}/{mults} ({rate_str})"
        return "-/- (0)"

    def get_station_details(self, callsign, contest):
        """Get station details and nearby competitors"""
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
                cursor.execute(query, (callsign, contest, contest))
                stations = cursor.fetchall()
                
                if not stations:
                    self.logger.error(f"No data found for {callsign} in {contest}")
                    return None
                
                return stations
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return None

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
                <td class="band-data">{qsos}/{mults}</td>
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

def main():
    parser = argparse.ArgumentParser(description='Generate contest score report')
    parser.add_argument('--callsign', required=True, help='Callsign to report')
    parser.add_argument('--contest', required=True, help='Contest name')
    parser.add_argument('--output-dir', required=True, help='Output directory for report')
    parser.add_argument('--db', default='contest_data.db', help='Database file path')
    parser.add_argument('--template', default='templates/score_template.html', 
                      help='Path to HTML template file')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Initialize reporter with provided paths
    reporter = ScoreReporter(args.db, args.template)
    
    # Set debug level if requested
    if args.debug:
        reporter.logger.setLevel(logging.DEBUG)
        reporter.logger.debug("Debug logging enabled")
        
    stations = reporter.get_station_details(args.callsign, args.contest)
    
    if stations:
        success = reporter.generate_html(args.callsign, args.contest, stations, args.output_dir)
        if not success:
            sys.exit(1)
    else:
        print(f"No data found for {args.callsign} in {args.contest}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
