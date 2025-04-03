#!/usr/bin/env python3
import sqlite3
import logging
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime
from callsign_utils import CallsignLookup
from batch_processor import BatchProcessor

class ContestDatabaseHandler:
    # Modified __init__ to accept socketio instance
    def __init__(self, db_path='contest_data.db', socketio=None):
        self.db_path = db_path
        self.socketio = socketio # Store socketio instance
        self.callsign_lookup = CallsignLookup()
        self.logger = logging.getLogger('ContestDatabaseHandler')
        self.setup_database()
        # Pass socketio instance to BatchProcessor
        self.batch_processor = BatchProcessor(self, self.socketio)
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
            conn.execute('''
                CREATE TABLE IF NOT EXISTS contest_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    contest TEXT,
                    callsign TEXT,
                    power TEXT,
                    assisted TEXT,
                    transmitter TEXT,
                    ops TEXT,
                    bands TEXT,
                    mode TEXT,
                    overlay TEXT,
                    club TEXT,
                    section TEXT,
                    score INTEGER,
                    qsos INTEGER,
                    multipliers INTEGER,
                    points INTEGER
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS band_breakdown (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contest_score_id INTEGER,
                    band TEXT,
                    mode TEXT,
                    qsos INTEGER,
                    points INTEGER,
                    multipliers INTEGER,
                    FOREIGN KEY (contest_score_id) REFERENCES contest_scores(id)
                )
            ''')
    
            conn.execute('''
                CREATE TABLE IF NOT EXISTS qth_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contest_score_id INTEGER,
                    dxcc_country TEXT,
                    cq_zone TEXT,
                    iaru_zone TEXT,
                    arrl_section TEXT,
                    state_province TEXT,
                    grid6 TEXT,
                    FOREIGN KEY (contest_score_id) REFERENCES contest_scores(id)
                )
            ''')

    def parse_xml_data(self, xml_data):
        """Parse XML data and return structured contest data."""
        xml_docs = re.findall(r'<\?xml.*?</dynamicresults>', xml_data, re.DOTALL)
        results = []
        self.logger.debug(f"Found {len(xml_docs)} XML document(s) in the batch.")

        for i, xml_doc in enumerate(xml_docs):
            self.logger.debug(f"--- Processing XML document {i+1} ---")
            # Log first few chars for identification without logging huge strings
            self.logger.debug(f"Raw XML snippet: {xml_doc[:200]}...")
            try:
                root = ET.fromstring(xml_doc)
                callsign = root.findtext('call', '')
                self.logger.debug(f"Extracted callsign: {callsign}")

                # Get QTH info
                qth_elem = root.find('qth')
                qth_data = {
                    'cq_zone': qth_elem.findtext('cqzone', ''),
                    'iaru_zone': qth_elem.findtext('iaruzone', ''),
                    'arrl_section': qth_elem.findtext('arrlsection', ''),
                    'state_province': qth_elem.findtext('stprvoth', ''),
                    'grid6': qth_elem.findtext('grid6', '')
                } if qth_elem is not None else {}
                self.logger.debug(f"Raw QTH data from XML: {qth_data}")

                # Get callsign info
                self.logger.debug(f"Looking up callsign info for: {callsign}")
                callsign_info = self.callsign_lookup.get_callsign_info(callsign)
                self.logger.debug(f"Callsign lookup result: {callsign_info}")
                if callsign_info:
                    # Enrich qth_data
                    qth_data['dxcc_country'] = callsign_info.get('prefix', qth_data.get('dxcc_country')) # Keep original if lookup fails
                    qth_data['continent'] = callsign_info.get('continent', qth_data.get('continent'))
                    # Only overwrite if missing or zero in original data
                    if not qth_data.get('cq_zone') or qth_data.get('cq_zone') == '0':
                        qth_data['cq_zone'] = callsign_info.get('cq_zone')
                    if not qth_data.get('iaru_zone') or qth_data.get('iaru_zone') == '0':
                        qth_data['iaru_zone'] = callsign_info.get('itu_zone')
                    self.logger.debug(f"Enriched QTH data: {qth_data}")

                # Extract contest data
                self.logger.debug("Extracting main contest data...")
                contest_data = self._extract_contest_data(root, callsign, qth_data)
                self.logger.debug(f"Final structured data for this doc: {contest_data}")
                results.append(contest_data)
                self.logger.debug(f"--- Finished processing XML document {i+1} ---")

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
            'score': int(float(root.findtext('score', 0))),
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
        cursor.execute('''
            INSERT INTO qth_info (
                contest_score_id, dxcc_country, continent, cq_zone, 
                iaru_zone, arrl_section, state_province, grid6
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
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
                    cursor.execute('''
                        INSERT INTO contest_scores (
                            timestamp, contest, callsign, power, assisted, transmitter,
                            ops, bands, mode, overlay, club, section, score, qsos,
                            multipliers, points
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
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
            cursor.execute('''
                INSERT INTO band_breakdown (
                    contest_score_id, band, mode, qsos, points, multipliers
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                contest_score_id,
                band_data['band'],
                band_data['mode'],
                band_data['qsos'],
                band_data['points'],
                band_data['multipliers']
            ))
