import plistlib
import logging
from typing import Dict, Optional

class CallsignLookup:
    def __init__(self, plist_path: str = "cty.plist"):
        """Initialize with cty.plist data and empty cache."""
        self.cty_list = self._load_plist(plist_path)
        self._cache: Dict[str, dict] = {}
        
    def get_callsign_info(self, callsign: str) -> Optional[dict]:
        """Get callsign information with improved prefix matching."""
        # Check cache first
        if callsign in self._cache:
            return self._cache[callsign]
        
        # Extract base callsign (remove any /P, /M etc)
        base_call = callsign.split('/')[0]
        
        # First try exact prefix match
        for length in range(min(len(base_call), 4), 0, -1):
            prefix = base_call[:length]
            if prefix in self.cty_list:
                info = self.cty_list[prefix]
                result = {
                    "prefix": prefix,  # Store actual prefix, not full callsign
                    "country": info.get("Country", ""),
                    "continent": info.get("Continent", ""),
                    "adif": info.get("ADIF", 0),
                    "cq_zone": info.get("CQZone", 0),
                    "itu_zone": info.get("ITUZone", 0),
                    "latitude": info.get("Latitude", 0.0),
                    "longitude": info.get("Longitude", 0.0)
                }
                # Cache result for the full callsign
                self._cache[callsign] = result
                return result
        
        # If no match found, cache None to avoid repeated lookups
        self._cache[callsign] = None
        return None
    
    def clear_cache(self) -> None:
        """Clear the lookup cache."""
        self._cache.clear()

    def _load_plist(self, plist_path: str) -> dict:
        """Load the cty.plist file."""
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
        """Get just the country name for a callsign."""
        info = self.get_callsign_info(callsign)
        return info["country"] if info else None
        
    def get_continent(self, callsign: str) -> Optional[str]:
        """Get just the continent for a callsign."""
        info = self.get_callsign_info(callsign)
        return info["continent"] if info else None
    
