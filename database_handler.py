#!/usr/bin/env python3
import sqlite3
import logging
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime
from callsign_utils import CallsignLookup
from shared_processor import shared_processor
from sql_queries import (
    CREATE_CONTEST_SCORES_TABLE,
    CREATE_BAND_BREAKDOWN_TABLE,
    CREATE_QTH_INFO_TABLE,
    INSERT_QTH_INFO,
    INSERT_BAND_BREAKDOWN,
    INSERT_CONTEST_DATA
)

class ContestDatabaseHandler:
    def __init__(self, db_path='contest_data.db'):
        self.db_path = db_path
        self.callsign_lookup = CallsignLookup()
        self.logger = logging.getLogger('ContestDatabaseHandler')
        self.setup_database()
        self.batch_processor = shared_processor
        self.batch_processor.set_handler(self)
        self.batch_processor.start()

    def process_submission(self, xml_data):
        """Add submission to batch instead of processing immediately"""
        self.batch_processor.add_to_batch(xml_data)

    def cleanup(self):
        """Cleanup resources"""
        self.batch_processor.stop()

    def setup_database(self):
        """Create the database tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(CREATE_CONTEST_SCORES_TABLE)
            conn.execute(CREATE_BAND_BREAKDOWN_TABLE)
            conn.execute(CREATE_QTH_INFO_TABLE)

    def parse_xml_data(self, xml_data):
        """Parse XML data and return structured contest data."""
        xml_docs = re.findall(r'<\?xml.*?</dynamicresults>', xml_data, re.DOTALL)
        results = []
        
        for xml_doc in xml_docs:
            try:
                root = ET.fromstring(xml_doc)
                callsign = root.findtext('call', '')
                
                # Get QTH info
                qth_elem = root.find('qth')
                qth_data = {
                    'cq_zone': qth_elem.findtext('cqzone', ''),
                    'iaru_zone': qth_elem.findtext('iaruzone', ''),
                    'arrl_section': qth_elem.findtext('arrlsection', ''),
                    'state_province': qth_elem.findtext('stprvoth', ''),
                    'grid6': qth_elem.findtext('grid6', '')
                } if qth_elem is not None else {}
                
                # Get callsign info
                callsign_info = self.callsign_lookup.get_callsign_info(callsign)
                if callsign_info:
                    qth_data['dxcc_country'] = callsign_info['prefix']
                    qth_data['continent'] = callsign_info['continent']
                    if qth_data.get('cq_zone') in [None, '', '0']:
                        qth_data['cq_zone'] = callsign_info['cq_zone']
                    if qth_data.get('iaru_zone') in [None, '', '0']:
                        qth_data['iaru_zone'] = callsign_info['itu_zone']
                
                # Extract contest data
                contest_data = self._extract_contest_data(root, callsign, qth_data)
                results.append(contest_data)
                
            except ET.ParseError as e:
                self.logger.error(f"Error parsing XML: {e}")
            except Exception as e:
                self.logger.error(f"Error processing data: {e}")
                self.logger.error(traceback.format_exc())
                
        return results

    def _extract_contest_data(self, root, callsign, qth_data):
        """Extract contest data from XML root element."""
        contest_data = {
            'contest': root.findtext('contest', ''),
            'callsign': callsign,
            'timestamp': root.findtext('timestamp', ''),
            'club': root.findtext('club', '').strip(),
            'section': root.find('.//qth/arrlsection').text if root.find('.//qth/arrlsection') is not None else '',
            'score': int(root.findtext('score', 0)),
            'qth': qth_data
        }

        # Extract class attributes
        class_elem = root.find('class')
        if class_elem is not None:
            contest_data.update({
                'power': class_elem.get('power', ''),
                'assisted': class_elem.get('assisted', ''),
                'transmitter': class_elem.get('transmitter', ''),
                'ops': class_elem.get('ops', ''),
                'bands': class_elem.get('bands', ''),
                'mode': class_elem.get('mode', '')
            })

        # Extract breakdown
        breakdown = root.find('breakdown')
        if breakdown is not None:
            contest_data.update(self._extract_breakdown_data(breakdown))

        return contest_data

    def _extract_breakdown_data(self, breakdown):
        """Extract breakdown data from XML breakdown element."""
        try:
            # Handle 'None' text value in XML safely
            def safe_int(elem_text, default=0):
                try:
                    return int(elem_text) if elem_text is not None else default
                except (ValueError, TypeError):
                    return default
    
            # Get total QSOs, points, and multipliers safely
            total_qsos = safe_int(breakdown.findtext('qso[@band="total"][@mode="ALL"]', 0))
            if total_qsos == 0:
                total_qsos = sum(safe_int(elem.text) for elem in breakdown.findall('qso[@band="total"]'))
    
            total_points = safe_int(breakdown.findtext('point[@band="total"][@mode="ALL"]', 0))
            if total_points == 0:
                total_points = sum(safe_int(elem.text) for elem in breakdown.findall('point[@band="total"]'))
    
            total_mults = safe_int(breakdown.findtext('mult[@band="total"][@mode="ALL"]', 0))
            if total_mults == 0:
                total_mults = sum(safe_int(elem.text) for elem in breakdown.findall('mult[@band="total"]'))
    
            data = {
                'qsos': total_qsos,
                'points': total_points,
                'multipliers': total_mults,
                'band_breakdown': []
            }
    
            # Extract per-band breakdown
            for band in ['160', '80', '40', '20', '15', '10']:
                qsos = sum(safe_int(elem.text) for elem in breakdown.findall(f'qso[@band="{band}"]'))
                points = sum(safe_int(elem.text) for elem in breakdown.findall(f'point[@band="{band}"]'))
                multipliers = sum(safe_int(elem.text) for elem in breakdown.findall(f'mult[@band="{band}"]'))
                
                if qsos > 0:
                    data['band_breakdown'].append({
                        'band': band,
                        'mode': 'ALL',
                        'qsos': qsos,
                        'points': points,
                        'multipliers': multipliers
                    })
    
            return data
        except Exception as e:
            self.logger.error(f"Error extracting breakdown data: {e}")
            self.logger.debug(f"Breakdown XML: {ET.tostring(breakdown, encoding='unicode')}")
            raise
    
    def _store_qth_info(self, cursor, contest_score_id, qth_data):
        """Store QTH information in database."""
        cursor.execute(INSERT_QTH_INFO, (
            contest_score_id,
            qth_data.get('dxcc_country', ''),
            qth_data.get('continent', ''),  # Fixed typo in variable name
            qth_data.get('cq_zone', ''),
            qth_data.get('iaru_zone', ''),
            qth_data.get('arrl_section', ''),
            qth_data.get('state_province', ''),
            qth_data.get('grid6', '')
        ))

    def store_data(self, contest_data):
        """Store contest data in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for data in contest_data:
                try:
                    # Insert main contest data
                    cursor.execute(INSERT_CONTEST_DATA, (
                        data['timestamp'], data['contest'], data['callsign'],
                        data.get('power', ''), data.get('assisted', ''),
                        data.get('transmitter', ''), data.get('ops', ''),
                        data.get('bands', ''), data.get('mode', ''),
                        data.get('overlay', ''), data['club'], data['section'],
                        data['score'], data.get('qsos', 0), data.get('multipliers', 0),
                        data.get('points', 0)
                    ))
                    
                    contest_score_id = cursor.lastrowid
                    
                    # Store QTH info
                    self._store_qth_info(cursor, contest_score_id, data['qth'])
                    
                    # Store band breakdown
                    self._store_band_breakdown(cursor, contest_score_id, data.get('band_breakdown', []))
                    
                    conn.commit()
                    
                except Exception as e:
                    self.logger.error(f"Error storing data for {data['callsign']}: {e}")
                    self.logger.error(traceback.format_exc())
                    raise

    
    def _store_band_breakdown(self, cursor, contest_score_id, band_breakdown):
        """Store band breakdown information in database."""
        for band_data in band_breakdown:
            cursor.execute(INSERT_BAND_BREAKDOWN, (
                contest_score_id,
                band_data['band'],
                band_data['mode'],
                band_data['qsos'],
                band_data['points'],
                band_data['multipliers']
            ))

    def get_scores(self, contest, callsign, filter_type, filter_value):
        """Get scores with optional filtering"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Base query
            query = """
                SELECT cs.*, qth.*
                FROM contest_scores cs
                LEFT JOIN qth_info qth ON cs.id = qth.contest_score_id
                WHERE cs.contest = ?
            """
            params = [contest]

            # Add callsign filter if provided
            if callsign:
                query += " AND cs.callsign = ?"
                params.append(callsign)

            # Add additional filters
            if filter_type and filter_value:
                if filter_type == 'dxcc':
                    query += " AND qth.dxcc_country = ?"
                elif filter_type == 'cq_zone':
                    query += " AND qth.cq_zone = ?"
                elif filter_type == 'iaru_zone':
                    query += " AND qth.iaru_zone = ?"
                elif filter_type == 'arrl_section':
                    query += " AND qth.arrl_section = ?"
                elif filter_type == 'state':
                    query += " AND qth.state_province = ?"
                elif filter_type == 'continent':
                    query += " AND qth.continent = ?"
                params.append(filter_value)

            cursor.execute(query, params)
            results = cursor.fetchall()

            # Format results
            scores = []
            for row in results:
                score = dict(row)
                # Get band breakdown
                cursor.execute("""
                    SELECT band, mode, qsos, points, multipliers
                    FROM band_breakdown
                    WHERE contest_score_id = ?
                """, (row['id'],))
                score['band_breakdown'] = {b['band']: b for b in cursor.fetchall()}
                scores.append(score)

            return {
                'contest': contest,
                'callsign': callsign,
                'stations': scores,
                'timestamp': self.get_last_update_timestamp()
            }

    def get_last_update_timestamp(self):
        """Get the timestamp of the most recent update"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MAX(timestamp) as last_update
                FROM contest_scores
            """)
            result = cursor.fetchone()
            return result[0] if result else datetime.now().isoformat()

    def get_station_details(self, conn, callsign, contest, filter_type=None, filter_value=None):
        """Get detailed station information for web interface"""
        scores = self.get_scores(contest, callsign, filter_type, filter_value)
        
        stations = []
        for station in scores['stations']:
            stations.append({
                'callsign': station['callsign'],
                'score': station['score'],
                'power': station['power'],
                'assisted': station['assisted'],
                'category': station['ops'],  # Using ops as category
                'bandData': {b['band']: f"{b['qsos']}/{b['multipliers']}" 
                            for b in station['band_breakdown']},
                'totalQsos': station['qsos'],
                'multipliers': station['multipliers'],
                'lastUpdate': station['timestamp'],
                'position': 0,  # Will be calculated by web interface
                'relativePosition': 'current' if station['callsign'] == callsign else 'below'
            })
            
        return {
            'contest': contest,
            'callsign': callsign,
            'stations': stations,
            'timestamp': scores['timestamp']
        }
