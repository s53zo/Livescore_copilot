import plistlib
import logging
from typing import Dict, Optional

class CallsignLookup:
    def __init__(self, plist_path: str = "cty.plist"):
        """Initialize the callsign lookup with cty.plist data."""
        self.cty_list = self._load_plist(plist_path)
        self._cache: Dict[str, dict] = {}
        
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

    def get_callsign_info(self, callsign: str) -> Optional[dict]:
        """
        Get callsign information from cty.plist, including country and continent.
        Uses prefix matching with caching for performance.
        
        Args:
            callsign: The callsign to look up
            
        Returns:
            Dictionary containing callsign information or None if not found
        """
        # Check cache first
        if callsign in self._cache:
            return self._cache[callsign]
        
        # If not in cache, try to find in cty.plist
        search_callsign = callsign
        while len(search_callsign) > 0:
            if search_callsign in self.cty_list:
                info = self.cty_list[search_callsign]
                result = {
                    "country": info.get("Country"),
                    "continent": info.get("Continent"),
                    "adif": info.get("ADIF"),
                    "cq_zone": info.get("CQZone"),
                    "itu_zone": info.get("ITUZone"),
                    "latitude": info.get("Latitude"),
                    "longitude": info.get("Longitude"),
                    "prefix": search_callsign
                }
                # Cache the result for future lookups
                self._cache[callsign] = result
                return result
            search_callsign = search_callsign[:-1]
        
        # Not found
        self._cache[callsign] = None
        return None

    def get_country(self, callsign: str) -> Optional[str]:
        """Get just the country name for a callsign."""
        info = self.get_callsign_info(callsign)
        return info["country"] if info else None
        
    def get_continent(self, callsign: str) -> Optional[str]:
        """Get just the continent for a callsign."""
        info = self.get_callsign_info(callsign)
        return info["continent"] if info else None
        
    def clear_cache(self) -> None:
        """Clear the lookup cache."""
        self._cache.clear()
