#!/usr/bin/env python3
import plistlib
import logging
from typing import Dict, Optional

class CallsignLookup:
    def __init__(self, plist_path: str = "cty.plist"):
        self.cty_list = self._load_plist(plist_path)
        self._cache: Dict[str, dict] = {}
        
    def get_callsign_info(self, callsign: str) -> Optional[dict]:
        """Get callsign info, always using the Prefix field from CTY.plist entry"""
        if callsign in self._cache:
            return self._cache[callsign]
        
        base_call = callsign.split('/')[0]
        info = None
        
        # First check for exact callsign match
        if base_call in self.cty_list:
            info = self.cty_list[base_call]
        else:
            # Then check for prefix match
            for i in range(len(base_call), 0, -1):
                prefix = base_call[:i]
                if prefix in self.cty_list:
                    info = self.cty_list[prefix]
                    break
        
        if info:
            result = {
                "prefix": info.get("Prefix", ""),  # Use the Prefix field from CTY.plist
                "country": info.get("Country", ""),
                "continent": info.get("Continent", ""),
                "adif": info.get("ADIF", 0),
                "cq_zone": info.get("CQZone", 0),
                "itu_zone": info.get("ITUZone", 0),
                "latitude": info.get("Latitude", 0.0),
                "longitude": info.get("Longitude", 0.0)
            }
            self._cache[callsign] = result
            return result
        
        self._cache[callsign] = None
        return None

    def clear_cache(self) -> None:
        self._cache.clear()

    def _load_plist(self, plist_path: str) -> dict:
        try:
            with open(plist_path, 'rb') as file:
                return plistlib.load(file)
        except FileNotFoundError:
            logging.error(f"Error: {plist_path} not found")
            raise
        except Exception as e:
            logging.error(f"Error loading {plist_path}: {e}")
            raise
    
    def get_country(self, callsign: str) -> Optional[str]:
        info = self.get_callsign_info(callsign)
        return info["country"] if info else None
        
    def get_continent(self, callsign: str) -> Optional[str]:
        info = self.get_callsign_info(callsign)
        return info["continent"] if info else None
