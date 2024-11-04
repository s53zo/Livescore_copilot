#!/usr/bin/env python3
import argparse
import sqlite3
import os
import sys
from datetime import datetime
import logging

class ScoreReporter:
    def __init__(self, db_path='contest_data.db', template_path='templates/contest_template.html'):
        self.db_path = db_path
        self.template_path = template_path
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('ScoreReporter')

    def load_template(self):
        """Load HTML template from file"""
        try:
            with open(self.template_path, 'r') as f:
                return f.read()
        except IOError as e:
            self.logger.error(f"Error loading template: {e}")
            return None

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
                return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return None

    def get_band_breakdown(self, station_id):
        """Get band breakdown for a station"""
        query = """
            SELECT band, qsos, multipliers
            FROM band_breakdown
            WHERE contest_score_id = ?
            ORDER BY CASE band
                WHEN '160' THEN 1
                WHEN '80' THEN 2
                WHEN '40' THEN 3
                WHEN '20' THEN 4
                WHEN '15' THEN 5
                WHEN '10' THEN 6
                ELSE 7
            END;
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, (station_id,))
                return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            return {}

    def format_band_data(self, band_breakdown, band):
        """Format band data as QSO/Mults"""
        if band in band_breakdown:
            qsos, mults = band_breakdown[band]
            return f"{qsos}/{mults}"
        return "-/-"

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
            band_breakdown = self.get_band_breakdown(station_id)
            
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
                <td class="band-data">{self.format_band_data(band_breakdown, '160')}</td>
                <td class="band-data">{self.format_band_data(band_breakdown, '80')}</td>
                <td class="band-data">{self.format_band_data(band_breakdown, '40')}</td>
                <td class="band-data">{self.format_band_data(band_breakdown, '20')}</td>
                <td class="band-data">{self.format_band_data(band_breakdown, '15')}</td>
                <td class="band-data">{self.format_band_data(band_breakdown, '10')}</td>
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
    parser.add_argument('--template', default='templates/contest_template.html', 
                      help='Path to HTML template file')
    
    args = parser.parse_args()
    
    reporter = ScoreReporter(args.db, args.template)
    stations = reporter.get_station_details(args.callsign, args.contest)
    
    if stations:
        success = reporter.generate_html(args.callsign, args.contest, stations, args.output_dir)
        if not success:
            sys.exit(1)
    else:
        print(f"No data found for {callsign} in {contest}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
