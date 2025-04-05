#!/usr/bin/env python3
import sqlite3
import logging
import xml.etree.ElementTree as ET
import re
import traceback
from datetime import datetime
import json # Added for MQTT payload
import paho.mqtt.client as mqtt # Added for MQTT
from paho.mqtt.client import CallbackAPIVersion # Added for MQTT
from callsign_utils import CallsignLookup
from batch_processor import BatchProcessor

class ContestDatabaseHandler:
    # Added mqtt_config parameter with default None
    def __init__(self, db_path='contest_data.db', mqtt_config=None):
        self.db_path = db_path
        self.callsign_lookup = CallsignLookup()
        self.logger = logging.getLogger('ContestDatabaseHandler')
        self.setup_database()
        self.batch_processor = BatchProcessor(self)
        self.batch_processor.start()
        self.mqtt_config = mqtt_config # Store MQTT config
        self.mqtt_client = None # Initialize MQTT client attribute
        if self.mqtt_config:
            self._setup_mqtt() # Initialize MQTT if config provided

    def process_submission(self, xml_data):
        """Add submission to batch instead of processing immediately"""
        self.batch_processor.add_to_batch(xml_data)

    def _setup_mqtt(self):
        """Initialize and connect the MQTT client."""
        if not self.mqtt_config or not self.mqtt_config.get('host'):
            self.logger.warning("MQTT configuration missing or incomplete, MQTT disabled.")
            return

        self.logger.info("Setting up MQTT client in DatabaseHandler...")
        try:
            self.mqtt_client = mqtt.Client(
                client_id=self.mqtt_config.get('client_id'), # Allow custom client ID
                callback_api_version=CallbackAPIVersion.VERSION2
            )
            self.mqtt_client.on_connect = self._on_connect
            self.mqtt_client.on_disconnect = self._on_disconnect
            self.mqtt_client.on_publish = self._on_publish
            self.mqtt_client.on_log = self._on_mqtt_log

            # Enable MQTT client logging if logger is in debug mode
            if self.logger.getEffectiveLevel() <= logging.DEBUG:
                 self.mqtt_client.enable_logger(self.logger)

            if self.mqtt_config.get('username'):
                self.logger.debug("Configuring MQTT authentication")
                self.mqtt_client.username_pw_set(
                    self.mqtt_config['username'],
                    self.mqtt_config.get('password')
                )

            if self.mqtt_config.get('use_tls', False): # Default to False if not specified
                self.logger.debug("Configuring TLS for MQTT connection")
                # Add more TLS options here if needed (ca_certs, certfile, keyfile)
                self.mqtt_client.tls_set()

            host = self.mqtt_config['host']
            port = self.mqtt_config.get('port', 1883) # Default port
            self.logger.info(f"Connecting to MQTT broker at {host}:{port}")
            self.mqtt_client.connect(host, port)
            self.mqtt_client.loop_start()
            self.logger.info("MQTT client started successfully in DatabaseHandler")

        except Exception as e:
            self.logger.error(f"Failed to setup/connect MQTT client: {e}")
            self.logger.debug(traceback.format_exc())
            self.mqtt_client = None # Ensure client is None if setup fails

    # --- MQTT Callbacks ---
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """MQTT connection callback."""
        rc_codes = {
            0: "Connected successfully", 131: "Malformed Packet", 132: "Protocol Error",
            135: "Not authorized", 148: "Server unavailable", 151: "Bad user name or password",
        }
        if reason_code == 0:
            self.logger.info(f"MQTT client connected to {self.mqtt_config['host']}:{self.mqtt_config.get('port', 1883)}")
        else:
            error_message = rc_codes.get(reason_code, f'Unknown error code {reason_code}')
            self.logger.error(f"MQTT connection failed: {error_message}")

    def _on_disconnect(self, client, userdata, reason_code, properties):
        """MQTT disconnection callback."""
        if reason_code == 0:
            self.logger.info("MQTT client cleanly disconnected")
        else:
            self.logger.warning(f"MQTT client unexpectedly disconnected with reason code: {reason_code}")
            # Optional: Add reconnection logic here if needed, but loop_start handles basic retries

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """MQTT publish callback."""
        self.logger.debug(f"MQTT Message {mid} published (Reason Code: {reason_code})")

    def _on_mqtt_log(self, client, userdata, level, buf):
        """MQTT client internal logging callback."""
        # Map paho levels to Python logging levels if desired, or log directly
        self.logger.debug(f"MQTT Log: {buf}")
    # --- End MQTT Callbacks ---

    def cleanup(self):
        """Cleanup resources"""
        self.batch_processor.stop()
        if self.mqtt_client:
            try:
                self.logger.info("Stopping MQTT client in DatabaseHandler...")
                # Check if connected before disconnecting cleanly
                # Note: is_connected() might not be reliable if loop isn't running
                self.mqtt_client.loop_stop() # Stop network loop first
                self.mqtt_client.disconnect()
                self.logger.info("MQTT client stopped in DatabaseHandler")
            except Exception as e:
                 self.logger.error(f"Error stopping MQTT client: {e}")
                 self.logger.debug(traceback.format_exc())
    # --- End MQTT Callbacks --- # Corrected indentation

    # --- MQTT Helper Methods --- # Corrected indentation for the whole block
    def _build_topic(self, data):
        """Build comprehensive MQTT topic including QTH and operating parameters."""

        # Helper function to get value or 'NA', and sanitize slightly
        def get_safe_field(value, default='NA'):
            # Convert to string, strip whitespace, handle None
            safe_value = str(value).strip() if value is not None else ''
            # Return the value if it's not empty, otherwise the default
            return safe_value if safe_value else default
            # Note: Further sanitization (e.g., removing special chars) might be needed
            # depending on the range of possible values, but we'll keep it simple for now.

        # Extract fields using the helper
        contest = get_safe_field(data.get('contest')).replace(' ', '_') # Replace spaces in contest name

        qth_data = data.get('qth', {}) # Get QTH dict safely
        dxcc = get_safe_field(qth_data.get('dxcc_country'))
        cqz = get_safe_field(qth_data.get('cq_zone'))

        grid = get_safe_field(qth_data.get('grid'))
        # Take first 2 chars of grid if available and seems valid (letters/numbers), else 'NA'
        grid2 = grid[:2] if len(grid) >= 2 and grid[:2].isalnum() else 'NA'

        power = get_safe_field(data.get('power'))
        assisted = get_safe_field(data.get('assisted'))

        callsign = get_safe_field(data.get('callsign'))
        callsign_safe = callsign.replace('/', '_') # Specifically handle '/' in callsigns

        # Construct the topic
        return f"contest/live/v1/{contest}/{dxcc}/{cqz}/{grid2}/{power}/{assisted}/{callsign_safe}"

    def _build_payload(self, data, contest_score_id):
        """Build JSON payload from contest data dictionary."""
        try:
            # Safely parse timestamp or use current time
            timestamp_dt = datetime.now()
            if data.get('timestamp'):
                try:
                    # Assuming timestamp format is like 'YYYY-MM-DD HH:MM:SS'
                    timestamp_dt = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not parse timestamp '{data['timestamp']}' for {data.get('callsign', 'unknown')}, using current time.")

            payload = {
                "sq": contest_score_id, # Use the ID generated during insert
                "t": int(timestamp_dt.timestamp()),
                "contest": data.get('contest', ''),
                "callsign": data.get('callsign', ''),
                "score": data.get('score', 0),
                "qsos": data.get('qsos', 0),
                "mults": data.get('multipliers', 0),
                "power": data.get('power', ''),
                "assisted": data.get('assisted', ''),
                "tx": data.get('transmitter', ''),
                "bands": {},
                "qth": {}
            }

            # Add band breakdown
            if data.get('band_breakdown'):
                for band_info in data['band_breakdown']:
                    band_key = f"{band_info.get('band', 'unknown')}m"
                    payload["bands"][band_key] = {
                        "mode": band_info.get('mode', 'ALL'),
                        "qsos": band_info.get('qsos', 0),
                        "points": band_info.get('points', 0),
                        "mults": band_info.get('multipliers', 0)
                    }

            # Add QTH info
            qth_data = data.get('qth', {})
            if qth_data:
                 payload["qth"] = {
                    "dxcc": qth_data.get('dxcc_country', ''),
                    "cqz": qth_data.get('cq_zone', ''),
                    "ituz": qth_data.get('iaru_zone', ''),
                    "section": qth_data.get('arrl_section', ''),
                    "state": qth_data.get('state_province', ''),
                    "grid": qth_data.get('grid6', '')
                }

            return json.dumps(payload)
        except Exception as e:
            self.logger.error(f"Error building MQTT payload for {data.get('callsign', 'unknown')}: {e}")
            self.logger.debug(traceback.format_exc())
            return None
    # --- End MQTT Helper Methods --- # Corrected indentation

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
        """Store contest data in the database and publish to MQTT."""
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

                    conn.commit() # Commit successful DB transaction for this record

                    # --- Publish to MQTT ---
                    if self.mqtt_client: # Check if MQTT client is initialized
                        try:
                            topic = self._build_topic(data)
                            payload = self._build_payload(data, contest_score_id)
                            if topic and payload:
                                self.logger.debug(f"Publishing to MQTT topic: {topic}")
                                info = self.mqtt_client.publish(topic, payload, qos=1)
                                # Check publish result (info.rc) for immediate errors
                                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                                     self.logger.error(f"Failed to queue MQTT message for {data.get('callsign', 'unknown')}, error code: {info.rc}")
                        except Exception as mqtt_e:
                            self.logger.error(f"Error publishing MQTT message for {data.get('callsign', 'unknown')}: {mqtt_e}")
                            self.logger.debug(traceback.format_exc())
                    # --- End MQTT Publish ---

                except Exception as e:
                    self.logger.error(f"Error storing data for {data['callsign']}: {e}")
                    self.logger.error(traceback.format_exc())
                    raise # Re-raise the exception after logging


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
