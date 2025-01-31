#!/usr/bin/env python3
import sqlite3
from sqlite3 import DatabaseError, OperationalError, IntegrityError
import logging
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime
from callsign_utils import CallsignLookup
from batch_processor import BatchProcessor
from contest_score_manager import ContestScoreManager
from sql_queries import (
    CREATE_CONTEST_SCORES_TABLE,
    CREATE_BAND_BREAKDOWN_TABLE,
    CREATE_QTH_INFO_TABLE
)

class ContestDatabaseHandler:
    def __init__(self, db_path='contest_data.db'):
        self.db_path = db_path
        self.callsign_lookup = CallsignLookup()
        self.logger = logging.getLogger('ContestDatabaseHandler')
        self.connection_pool = None
        self.batch_processor = BatchProcessor(self)
        self.score_manager = ContestScoreManager(db_path)

    def process_submission(self, xml_data):
        """Add submission to batch instead of processing immediately"""
        self.batch_processor.add_to_batch(xml_data)

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.batch_processor.stop()
            if self.connection_pool:
                self.connection_pool.close()
                self.logger.info("Database connection pool closed")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def setup_connection_pool(self):
        """Initialize database connection pool with WAL mode"""
        try:
            self.connection_pool = sqlite3.connect(self.db_path, check_same_thread=False)
            with self.connection_pool:
                self.connection_pool.execute("PRAGMA journal_mode=WAL")
                self.connection_pool.execute("PRAGMA foreign_keys=ON")
                self.connection_pool.execute("PRAGMA busy_timeout=5000")
                self.connection_pool.execute(CREATE_CONTEST_SCORES_TABLE)
                self.connection_pool.execute(CREATE_BAND_BREAKDOWN_TABLE)
                self.connection_pool.execute(CREATE_QTH_INFO_TABLE)
            self.logger.info("Database connection pool initialized")
            self.batch_processor.start()
        except OperationalError as e:
            self.logger.critical(f"Database connection failed: {e}")
            raise SystemExit(1) from e

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

    def store_data(self, contest_data):
        """Store contest data using score manager"""
        try:
            for data in contest_data:
                # Format data for score manager
                score_data = {
                    'timestamp': data['timestamp'],
                    'contest': data['contest'],
                    'callsign': data['callsign'],
                    'power': data.get('power', ''),
                    'assisted': data.get('assisted', ''),
                    'transmitter': data.get('transmitter', ''),
                    'ops': data.get('ops', ''),
                    'bands': data.get('bands', ''),
                    'mode': data.get('mode', ''),
                    'overlay': data.get('overlay', ''),
                    'club': data['club'],
                    'section': data['section'],
                    'score': data['score'],
                    'qsos': data.get('qsos', 0),
                    'multipliers': data.get('multipliers', 0),
                    'points': data.get('points', 0),
                    'band_breakdown': data.get('band_breakdown', []),
                    'qth': data['qth']
                }

                self.score_manager.insert_score(score_data)

        except Exception as e:
            self.logger.error(f"Error storing data: {e}")
            self.logger.error(traceback.format_exc())
            raise
