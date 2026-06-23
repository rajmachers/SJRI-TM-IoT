import websocket
import threading
import json
import time
import requests
import logging
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum

# IST Timezone utility
IST = timezone(timedelta(hours=5, minutes=30))

def get_ist_now() -> datetime:
    """Get current time in IST timezone"""
    return datetime.now(IST)

def format_ist_datetime(dt: datetime) -> str:
    """Format datetime to dd/mm/yyyy, hh:mm:ss am/pm in IST"""
    if dt.tzinfo is None:
        # Assume local time if no timezone info
        dt = dt.replace(tzinfo=IST)
    elif dt.tzinfo != IST:
        # Convert to IST if different timezone
        dt = dt.astimezone(IST)
    
    return dt.strftime("%d/%m/%Y, %I:%M:%S %p").lower()

def parse_kinesis_timestamp_to_ist(timestamp_str: str) -> str:
    """Convert Kinesis UTC timestamp to IST dd/mm/yyyy, hh:mm:ss am/pm format"""
    try:
        if timestamp_str:
            # Parse UTC timestamp (handles 'Z' suffix)
            utc_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            # Convert to IST
            ist_dt = utc_dt.astimezone(IST)
            return ist_dt.strftime("%d/%m/%Y, %I:%M:%S %p").lower()
    except Exception as e:
        logging.warning(f"[Timezone] Could not parse timestamp {timestamp_str}: {e}")
    
    # Fallback to current IST time
    return format_ist_datetime(get_ist_now())


def parse_datetime(timestamp_str: str) -> datetime:
    """Parse ISO-like timestamp strings into timezone-aware datetime."""
    if not timestamp_str:
        raise ValueError("timestamp is empty")

    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)

class LogLevel(Enum):
    """Supported logging levels"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


@dataclass
class AssetConfig:
    """Asset configuration with complete coordinate system"""
    asset_id: str
    tb_uuid: Optional[str] = None
    lat: float = 12.9310000
    lng: float = 77.6194000
    apos_x: float = 0.0
    apos_y: float = 0.0
    pos_x: float = 0.0
    pos_y: float = 0.0
    dept: Optional[str] = None
    label: Optional[str] = None
    gps_cord: Optional[str] = None
    img_cord: Optional[str] = None


@dataclass
class DeviceConfig:
    """Device configuration with all ThingsBoard attributes"""
    device_id: str
    tb_uuid: str
    tb_token: Optional[str] = None
    dept: Optional[str] = None
    device_profile: Optional[str] = None
    name: Optional[str] = None
    label: Optional[str] = None
    device_type: Optional[str] = None
    fencing_aps: Optional[str] = None


@dataclass
class TracData:
    """Cached trac positioning data"""
    ref: str
    pos: List[float]
    timestamp: str



class CacheManager:
    """Unified cache manager for Assets and Devices with complete attribute loading"""

    def reconcile_location_asset_counts(self):
        """Ensure location asset counts match real-time device positions"""
        import logging
        logging.warning("[RECONCILE TEST] This is the updated reconcile_location_asset_counts method on server.")
        # Reconcile using location_occupancy (anchor_id -> [device_ids])
        import logging
        mismatches = []
        for location, device_list in self.location_occupancy.items():
            actual_count = len(device_list)
            logging.info(f"[RECONCILE] Location {location} has {actual_count} devices: {device_list}")
            # If you have an expected/summary count, compare here and add to mismatches
            # For now, just log the occupancy
        return mismatches
    
    def __init__(self, tb_client):
        self.tb_client = tb_client
        self.assets: Dict[str, AssetConfig] = {}
        self.devices: Dict[str, DeviceConfig] = {}
        self.trac_cache: Dict[str, TracData] = {}
        # Location analytics tracking
        self.device_locations: Dict[str, str] = {}  # device_id -> current_anchor_id
        self.location_occupancy: Dict[str, List[str]] = {}  # anchor_id -> [device_ids]
        self.device_visit_times: Dict[str, datetime] = {}  # device_id -> arrival_time
        self.daily_visits: Dict[str, int] = {}  # device_id -> visit_count_today
        self.pending_location_changes: Dict[str, Dict[str, Any]] = {}  # device_id -> {anchor_id, since}
        self.ap_change_debounce_seconds: int = int(self.tb_client.config.get('AP_CHANGE_DEBOUNCE_SECONDS', 60))
        
        # Enhanced analytics tracking
        self.location_daily_visits: Dict[str, int] = {}  # anchor_id -> total_visit_count_today (how many times devices came to this location)
        self.location_peak_occupancy: Dict[str, int] = {}  # anchor_id -> max_devices_simultaneously_today
        self.device_usage_sessions: Dict[str, datetime] = {}  # device_id -> session_start_time
        self.last_reset_date: Optional[str] = None  # Track last daily reset
        
    def load_all_assets(self) -> bool:
        """Load ALL Asset server attributes from ThingsBoard"""
        try:
            self.assets.clear()
            page = 0
            page_size = 20
            has_next = True
            assets_loaded = 0
            
            logging.info("[Cache] Loading all Asset attributes...")
            
            while has_next:
                res = requests.get(
                    f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/tenant/assetInfos",
                    headers=self.tb_client.get_tb_headers(),
                    params={
                        "pageSize": page_size,
                        "page": page,
                        "sortProperty": "createdTime",
                        "sortOrder": "DESC"
                    }
                )
                
                if res.status_code != 200:
                    logging.error(f"[Cache] Failed to fetch assets: {res.status_code} - {res.text}")
                    return False
                    
                data = res.json()
                for asset in data.get("data", []):
                    tb_asset_id = asset.get("id", {}).get("id")
                    asset_id = asset.get("name")
                    
                    # Debug: Log all asset fields available in the API response
                    logging.info(f"[Cache] Asset {asset_id} raw fields from API: {asset}")
                    
                    if not asset_id or not tb_asset_id:
                        continue
                    
                    # Extract Label from asset object (not server attributes)
                    asset_label = None
                    asset_type = asset.get("type")
                    additional_info = asset.get("additionalInfo", {})
                    
                    # Try multiple locations for the label
                    asset_label = (
                        asset.get("label") or 
                        asset.get("Label") or 
                        additional_info.get("label") or 
                        additional_info.get("Label") or
                        additional_info.get("description")
                    )
                    
                    logging.info(f"[Cache] Asset {asset_id} label extraction: label='{asset_label}', type='{asset_type}', additionalInfo keys: {list(additional_info.keys()) if additional_info else 'None'}")
                        
                    # Load ALL server-side attributes for coordinates
                    attrs = self._load_asset_attributes(tb_asset_id, asset_id)
                    if attrs:
                        # Create AssetConfig with coordinates from server attributes and label from asset fields
                        asset_config = AssetConfig(
                            asset_id=asset_id,
                            tb_uuid=tb_asset_id,
                            lat=float(attrs.get("lat", 12.9310000)),
                            lng=float(attrs.get("lng", 77.6194000)),
                            apos_x=float(attrs.get("aposX", 0.0)),
                            apos_y=float(attrs.get("aposY", 0.0)),
                            pos_x=float(attrs.get("posX", 0.0)),
                            pos_y=float(attrs.get("posY", 0.0)),
                            dept=attrs.get("Dept"),
                            label=asset_label,  # Use label from asset object, not server attributes
                            gps_cord=attrs.get("gpsCord"),
                            img_cord=attrs.get("imgCord")
                        )
                        
                        self.assets[asset_id] = asset_config
                        assets_loaded += 1
                        
                        # Debug log to show the actual label being loaded
                        logging.info(f"[Cache] ✅ Asset loaded: {asset_id}-{asset_label} with coordinates (Label: '{asset_label}', Dept: '{attrs.get('Dept')}', Type: '{asset_type}')")
                        
                has_next = data.get("hasNext", False)
                page += 1
                
            logging.info(f"[Cache] Successfully loaded {assets_loaded} assets with complete attributes")
            return True
            
        except Exception as e:
            logging.error(f"[Cache] Error loading assets: {e}")
            return False
    
    def _load_asset_attributes(self, tb_asset_id: str, asset_id: str) -> Optional[Dict]:
        """Load all server-side attributes for an asset and generate coordinate JSON fields"""
        try:
            url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/ASSET/{tb_asset_id}/values/attributes/SERVER_SCOPE"
            response = requests.get(url, headers=self.tb_client.get_tb_headers())
            
            if response.status_code != 200:
                logging.error(f"[Cache] Failed to load attributes for {asset_id}: {response.status_code}")
                return None
                
            attr_list = response.json()
            attrs = {attr["key"]: attr["value"] for attr in attr_list}
            
            # Generate JSON coordinate fields
            self._generate_coordinate_fields(attrs, asset_id)
            
            # Debug log to show all attributes received from ThingsBoard
            logging.info(f"[Cache] Asset {asset_id} attributes from TB: {attrs}")
            
            # Log missing critical coordinates
            missing_coords = [k for k in ["lat", "lng", "aposX", "aposY"] if k not in attrs]
            if missing_coords:
                logging.warning(f"[Cache] Asset {asset_id} missing coordinates: {missing_coords}")
                
            return attrs
            
        except Exception as e:
            logging.error(f"[Cache] Error loading attributes for {asset_id}: {e}")
            return None
    
    def _generate_coordinate_fields(self, attrs: Dict, asset_id: str) -> None:
        """Generate aimgCord, fimgCord, and gpsCord JSON fields from coordinates"""
        import json
        
        try:
            # Generate aimgCord from aposX, aposY
            apos_x = attrs.get("aposX")
            apos_y = attrs.get("aposY")
            if apos_x is not None and apos_y is not None:
                aimgCord = {
                    "lat": float(apos_x),
                    "lng": float(apos_y),
                    "radius": 10
                }
                attrs["aimgCord"] = json.dumps(aimgCord)
                logging.debug(f"[Cache] Generated aimgCord for {asset_id}: {attrs['aimgCord']}")
            
            # Generate fimgCord from posX, posY
            pos_x = attrs.get("posX")
            pos_y = attrs.get("posY")
            if pos_x is not None and pos_y is not None:
                fimgCord = {
                    "lat": float(pos_x),
                    "lng": float(pos_y),
                    "radius": 10
                }
                attrs["fimgCord"] = json.dumps(fimgCord)
                logging.debug(f"[Cache] Generated fimgCord for {asset_id}: {attrs['fimgCord']}")
            
            # Generate gpsCord from lat, lng
            lat = attrs.get("lat")
            lng = attrs.get("lng")
            if lat is not None and lng is not None:
                gpsCord = {
                    "lat": float(lat),
                    "lng": float(lng),
                    "radius": 10
                }
                attrs["gpsCord"] = json.dumps(gpsCord)
                logging.debug(f"[Cache] Generated gpsCord for {asset_id}: {attrs['gpsCord']}")
                
            logging.info(f"[Cache] ✅ Coordinate generation completed for asset {asset_id}")
            
        except Exception as e:
            logging.error(f"[Cache] Error generating coordinates for {asset_id}: {e}")
    
    def load_all_devices(self) -> bool:
        """Load ALL Device server attributes from ThingsBoard"""
        try:
            self.devices.clear()
            device_type = self.tb_client.config.get('DEVICE_TYPE', 'PoC-Tag')
            
            url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/tenant/devices"
            params = {"deviceType": device_type, "pageSize": 100, "page": 0}
            
            response = requests.get(url, headers=self.tb_client.get_tb_headers(), params=params)
            
            if response.status_code != 200:
                logging.error(f"[Cache] Failed to fetch devices: {response.status_code}")
                return False
                
            devices = response.json().get("data", [])
            devices_loaded = 0
            
            logging.info(f"[Cache] Loading {len(devices)} devices with complete attributes...")
            
            for device in devices:
                device_id = device.get("name")
                tb_uuid = device.get("id", {}).get("id")
                device_label = device.get("label") or device.get("Label")

                if not device_id or not tb_uuid:
                    continue

                # Load ALL device server-side attributes
                attrs = self._load_device_attributes(tb_uuid, device_id)
                if attrs:
                    device_config = DeviceConfig(
                        device_id=device_id,
                        tb_uuid=tb_uuid,
                        dept=attrs.get("Dept"),
                        device_profile=attrs.get("DeviceProfile"),
                        name=attrs.get("Name"),
                        label=device_label or attrs.get("label") or attrs.get("Label"),
                        device_type=attrs.get("DeviceType"),
                        fencing_aps=attrs.get("FencingAPs") or attrs.get("FencingAps")
                    )
                    
                    # Get device credentials token
                    device_config.tb_token = self._get_device_token(tb_uuid, device_id)
                    
                    self.devices[device_id] = device_config
                    devices_loaded += 1
                    
                    logging.info(f"[Cache] ✅ Device loaded: {device_id} (Name: '{attrs.get('Name', 'No Name')}', DeviceType: '{attrs.get('DeviceType', 'No Type')}') with fencing: {device_config.fencing_aps}")
                    
            logging.info(f"[Cache] Successfully loaded {devices_loaded} devices with complete attributes")
            return True
            
        except Exception as e:
            logging.error(f"[Cache] Error loading devices: {e}")
            return False
    
    def _load_device_attributes(self, tb_uuid: str, device_id: str) -> Optional[Dict]:
        """Load all server-side attributes for a device"""
        try:
            url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/DEVICE/{tb_uuid}/values/attributes/SERVER_SCOPE"
            response = requests.get(url, headers=self.tb_client.get_tb_headers())
            
            if response.status_code != 200:
                logging.error(f"[Cache] Failed to load attributes for device {device_id}: {response.status_code}")
                return None
                
            attr_list = response.json()
            attrs = {attr["key"]: attr["value"] for attr in attr_list}
            
            return attrs
            
        except Exception as e:
            logging.error(f"[Cache] Error loading attributes for device {device_id}: {e}")
            return None
    
    def _get_device_token(self, tb_uuid: str, device_id: str) -> Optional[str]:
        """Get ThingsBoard access token for device"""
        try:
            url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/device/{tb_uuid}/credentials"
            response = requests.get(url, headers=self.tb_client.get_tb_headers())
            
            if response.status_code == 200:
                token = response.json().get("credentialsId")
                logging.info(f"[Cache] ✅ Token retrieved for device {device_id}")
                return token
            else:
                logging.error(f"[Cache] ❌ Failed to get token for {device_id}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"[Cache] ❌ Error getting token for {device_id}: {e}")
            return None
    
    def _classify_device_type(self, device_type_attr: str) -> tuple:
        """Classify device based on DeviceType server attribute"""
        if not device_type_attr:
            return "Unknown", "Unknown"
        
        device_type_lower = device_type_attr.lower()
        
        # Map ThingsBoard DeviceType values to our classifications
        if "wheelchair" in device_type_lower or "chair" in device_type_lower:
            return "Wheelchair", "Mobility"
        elif "pulse" in device_type_lower or "oximeter" in device_type_lower or "spo2" in device_type_lower:
            return "Pulse Oximeter", "Monitoring"
        elif "bp" in device_type_lower or "blood" in device_type_lower or "pressure" in device_type_lower:
            return "BP Monitor", "Diagnostic"
        elif "monitor" in device_type_lower:
            return "Monitor", "Monitoring"
        elif "stretcher" in device_type_lower or "gurney" in device_type_lower:
            return "Stretcher", "Transport"
        elif "pump" in device_type_lower or "infusion" in device_type_lower:
            return "Infusion Pump", "Treatment"
        elif "ventilator" in device_type_lower or "respirator" in device_type_lower:
            return "Ventilator", "Critical Care"
        else:
            # Use the DeviceType value directly if it doesn't match patterns
            return device_type_attr, "Equipment"
    
    def _classify_device_name(self, device_name: str) -> tuple:
        """Classify device based on name attribute"""
        if not device_name:
            return "Unknown", "Unknown"
        
        name_lower = device_name.lower()
        
        if any(keyword in name_lower for keyword in ["wheelchair", "wheel chair", "chair"]):
            return "Wheelchair", "Mobility"
        elif any(keyword in name_lower for keyword in ["pulse", "oximeter", "spo2", "oxygen"]):
            return "Pulse Oximeter", "Monitoring"
        elif any(keyword in name_lower for keyword in ["bp", "blood pressure", "pressure"]):
            return "BP Monitor", "Diagnostic"
        elif any(keyword in name_lower for keyword in ["monitor", "display"]):
            return "Monitor", "Monitoring"
        elif any(keyword in name_lower for keyword in ["stretcher", "gurney"]):
            return "Stretcher", "Transport"
        elif any(keyword in name_lower for keyword in ["pump", "infusion"]):
            return "Infusion Pump", "Treatment"
        elif any(keyword in name_lower for keyword in ["ventilator", "respirator"]):
            return "Ventilator", "Critical Care"
        else:
            # Use device name directly if meaningful
            return device_name if len(device_name) > 2 else "Unknown Equipment", "Equipment"
    
    def _classify_device_id(self, device_id: str) -> tuple:
        """Classify device based on device ID patterns"""
        device_id_lower = device_id.lower()
        
        if any(pattern in device_id_lower for pattern in ["chair", "wheel"]):
            return "Wheelchair", "Mobility"
        elif any(pattern in device_id_lower for pattern in ["pulse", "ox", "spo2"]):
            return "Pulse Oximeter", "Monitoring"
        elif any(pattern in device_id_lower for pattern in ["bp", "pressure"]):
            return "BP Monitor", "Diagnostic"
        elif device_id.startswith(('d0', 'e0', 'f0')):
            return "Hospital Equipment", "Equipment"
        else:
            return f"Device-{device_id[:8]}", "Tracked Equipment"
    
    def _map_device_type_to_category(self, device_type_attr: str) -> tuple:
        """Map ThingsBoard DeviceType attribute to our classification system"""
        if not device_type_attr:
            return "Unknown", "Unknown"
        
        # Use DeviceType attribute directly, but map to appropriate category
        device_type_lower = device_type_attr.lower()
        
        # Determine category based on device type
        if any(keyword in device_type_lower for keyword in ["wheelchair", "chair", "mobility"]):
            category = "Mobility"
        elif any(keyword in device_type_lower for keyword in ["pulse", "oximeter", "monitor", "spo2"]):
            category = "Monitoring"
        elif any(keyword in device_type_lower for keyword in ["bp", "blood", "pressure", "diagnostic"]):
            category = "Diagnostic"
        elif any(keyword in device_type_lower for keyword in ["stretcher", "gurney", "transport"]):
            category = "Transport"
        elif any(keyword in device_type_lower for keyword in ["pump", "infusion", "treatment"]):
            category = "Treatment"
        elif any(keyword in device_type_lower for keyword in ["ventilator", "respirator", "critical"]):
            category = "Critical Care"
        else:
            category = "Equipment"
        
        # Return the actual DeviceType value (preserve original naming)
        return device_type_attr, category
    
    def _classify_from_name(self, device_name: str) -> tuple:
        """Classify device based on name attribute (fallback method)"""
        if not device_name or len(device_name) <= 2:
            return "Unknown Equipment", "Equipment"
        
        name_lower = device_name.lower()
        
        if any(keyword in name_lower for keyword in ["wheelchair", "wheel chair", "chair"]):
            return "Wheelchair", "Mobility"
        elif any(keyword in name_lower for keyword in ["pulse", "oximeter", "spo2", "oxygen"]):
            return "Pulse Oximeter", "Monitoring"
        elif any(keyword in name_lower for keyword in ["bp", "blood pressure", "pressure"]):
            return "BP Monitor", "Diagnostic"
        elif any(keyword in name_lower for keyword in ["monitor", "display"]):
            return "Monitor", "Monitoring"
        elif any(keyword in name_lower for keyword in ["stretcher", "gurney"]):
            return "Stretcher", "Transport"
        else:
            # Use device name directly if it's meaningful
            return device_name, "Equipment"
    
    def _classify_from_id(self, device_id: str) -> tuple:
        """Classify device based on device ID patterns (last resort)"""
        device_id_lower = device_id.lower()
        
        if any(pattern in device_id_lower for pattern in ["chair", "wheel"]):
            return "Wheelchair", "Mobility"
        elif any(pattern in device_id_lower for pattern in ["pulse", "ox", "spo2"]):
            return "Pulse Oximeter", "Monitoring"
        elif any(pattern in device_id_lower for pattern in ["bp", "pressure"]):
            return "BP Monitor", "Diagnostic"
        elif device_id.startswith(('d0', 'e0', 'f0')):
            return "Hospital Equipment", "Equipment"
        else:
            return f"Device-{device_id[:8]}", "Tracked Equipment"

    def get_device_type_for_analytics(self, device_id: str) -> str:
        """Get a short device type for analytics (used in asset telemetry)"""
        device_config = self.get_device_config(device_id)
        
        if device_config:
            # PRIORITY 1: Use DeviceType server attribute (most reliable)
            if device_config.device_type:
                return device_config.device_type  # Return the actual DeviceType value
            
            # PRIORITY 2: Fallback to name-based detection
            elif device_config.name:
                device_name = device_config.name.lower()
                if any(keyword in device_name for keyword in ["wheelchair", "wheel chair", "chair"]):
                    return "Wheelchair"
                elif any(keyword in device_name for keyword in ["pulse", "oximeter", "spo2"]):
                    return "PulseOx"
                elif any(keyword in device_name for keyword in ["bp", "blood pressure", "pressure"]):
                    return "BP-Monitor"
                elif any(keyword in device_name for keyword in ["monitor"]):
                    return "Monitor"
                elif any(keyword in device_name for keyword in ["stretcher"]):
                    return "Stretcher"
                else:
                    return "Equipment"
        
        # PRIORITY 3: Fallback to device ID pattern
        device_id_lower = device_id.lower()
        if any(pattern in device_id_lower for pattern in ["chair"]):
            return "Wheelchair"
        elif any(pattern in device_id_lower for pattern in ["pulse", "ox"]):
            return "PulseOx"
        elif device_id.startswith(('d0', 'e0', 'f0')):
            return "Hospital-Device"
        else:
            return "Equipment"

    def get_asset_config(self, asset_id: str) -> Optional[AssetConfig]:
        """Get cached asset configuration"""
        return self.assets.get(asset_id)
    
    def get_device_config(self, device_id: str) -> Optional[DeviceConfig]:
        """Get cached device configuration"""
        return self.devices.get(device_id)
    
    def is_device_allowed(self, device_id: str) -> bool:
        """Check if device is in allowed cache"""
        return device_id in self.devices
    
    def update_trac_cache(self, device_id: str, ref: str, pos: List[float], timestamp: str):
        """Update trac positioning cache"""
        self.trac_cache[device_id] = TracData(ref=ref, pos=pos, timestamp=timestamp)
        
    def get_trac_cache(self, device_id: str) -> Optional[TracData]:
        """Get cached trac data"""
        return self.trac_cache.get(device_id)
    
    def update_device_location(self, device_id: str, new_anchor_id: str) -> None:
        """Update device location with AP-change debouncing to avoid inflated visit counts from AP jitter."""
        now = get_ist_now()
        old_anchor_id = self.device_locations.get(device_id)

        # First observed location: accept immediately
        if old_anchor_id is None:
            if new_anchor_id not in self.location_occupancy:
                self.location_occupancy[new_anchor_id] = []
            if device_id not in self.location_occupancy[new_anchor_id]:
                self.location_occupancy[new_anchor_id].append(device_id)

            self.device_locations[device_id] = new_anchor_id
            self.device_visit_times[device_id] = now
            self.daily_visits[device_id] = self.daily_visits.get(device_id, 0) + 1
            self.location_daily_visits[new_anchor_id] = self.location_daily_visits.get(new_anchor_id, 0) + 1
            self.location_peak_occupancy[new_anchor_id] = max(
                self.location_peak_occupancy.get(new_anchor_id, 0),
                len(self.location_occupancy[new_anchor_id])
            )
            logging.info(f"[Analytics] ✅ Initial location for {device_id} set to {new_anchor_id}, visit #{self.daily_visits[device_id]}")
            self.pending_location_changes.pop(device_id, None)
            mismatches = self.reconcile_location_asset_counts()
            if mismatches:
                logging.warning(f"[RECONCILE] Mismatches found: {mismatches}")
            else:
                logging.info("[RECONCILE] No mismatches found. Counts are correct.")
            return

        # Same AP as current location: clear pending jitter candidate and return
        if old_anchor_id == new_anchor_id:
            if device_id in self.pending_location_changes:
                logging.debug(f"[Debounce] Cleared pending AP change for {device_id}; remained at {old_anchor_id}")
                self.pending_location_changes.pop(device_id, None)
            return

        # Debounce AP change: require new AP to remain stable for configured duration
        pending = self.pending_location_changes.get(device_id)
        if not pending or pending.get("anchor_id") != new_anchor_id:
            self.pending_location_changes[device_id] = {
                "anchor_id": new_anchor_id,
                "since": now
            }
            logging.info(
                f"[Debounce] Candidate AP change for {device_id}: {old_anchor_id} -> {new_anchor_id}; "
                f"waiting {self.ap_change_debounce_seconds}s"
            )
            return

        stable_seconds = int((now - pending["since"]).total_seconds())
        if stable_seconds < self.ap_change_debounce_seconds:
            logging.debug(
                f"[Debounce] AP change pending for {device_id}: {old_anchor_id} -> {new_anchor_id}, "
                f"stable {stable_seconds}s/{self.ap_change_debounce_seconds}s"
            )
            return

        # Debounce satisfied: commit location change
        self.pending_location_changes.pop(device_id, None)

        # Always update device location and occupancy
        if old_anchor_id and old_anchor_id in self.location_occupancy:
            if device_id in self.location_occupancy[old_anchor_id]:
                self.location_occupancy[old_anchor_id].remove(device_id)
                logging.debug(f"[Analytics] Removed {device_id} from {old_anchor_id}, now has {len(self.location_occupancy[old_anchor_id])} devices")
        if new_anchor_id not in self.location_occupancy:
            self.location_occupancy[new_anchor_id] = []
        if device_id not in self.location_occupancy[new_anchor_id]:
            self.location_occupancy[new_anchor_id].append(device_id)
            logging.info(f"[Analytics] 📊 Location {new_anchor_id} now has {len(self.location_occupancy[new_anchor_id])} devices: {self.location_occupancy[new_anchor_id]}")
        # Always update tracking
        self.device_locations[device_id] = new_anchor_id
        # Only update arrival time if location changed
        if old_anchor_id != new_anchor_id:
            self.device_visit_times[device_id] = now
            # Increment device-level daily visit count
            self.daily_visits[device_id] = self.daily_visits.get(device_id, 0) + 1
            logging.info(f"[Analytics] ✅ Device {device_id} new visit #{self.daily_visits[device_id]} to {new_anchor_id}")
            # Increment location-level visit count
            self.location_daily_visits[new_anchor_id] = self.location_daily_visits.get(new_anchor_id, 0) + 1
            logging.info(f"[Analytics] 📈 Location {new_anchor_id} visit count: {self.location_daily_visits[new_anchor_id]}")
            # Update peak occupancy for this location
            current_occupancy = len(self.location_occupancy[new_anchor_id])
            self.location_peak_occupancy[new_anchor_id] = max(
                self.location_peak_occupancy.get(new_anchor_id, 0),
                current_occupancy
            )
            logging.debug(f"[Analytics] 🏢 Location {new_anchor_id} peak occupancy updated: {self.location_peak_occupancy[new_anchor_id]} devices")
        else:
            logging.debug(f"[Analytics] 🔄 Device {device_id} still at {new_anchor_id} - arrival time unchanged")

        # Always call reconciliation after any device location update
        mismatches = self.reconcile_location_asset_counts()
        if mismatches:
            logging.warning(f"[RECONCILE] Mismatches found: {mismatches}")
        else:
            logging.info("[RECONCILE] No mismatches found. Counts are correct.")
            # Trigger asset/location telemetry update for the new anchor/location
            if hasattr(self, 'send_asset_telemetry_to_thingsboard'):
                logging.info(f"[ASSET] Attempting to send asset telemetry for anchor {new_anchor_id} after device {device_id} location update.")
                try:
                    result = self.send_asset_telemetry_to_thingsboard(new_anchor_id)
                    if result:
                        logging.info(f"[ASSET] Asset telemetry sent successfully for anchor {new_anchor_id}.")
                    else:
                        logging.error(f"[ASSET] Asset telemetry failed for anchor {new_anchor_id}.")
                except Exception as e:
                    logging.error(f"[ASSET] Exception during asset telemetry for anchor {new_anchor_id}: {e}")
    
    def get_location_occupancy(self, anchor_id: str) -> Dict[str, Any]:
        """Get real-time occupancy data for a location"""
        devices = self.location_occupancy.get(anchor_id, [])
        return {
            "current_device_count": len(devices),
            "devices_present": devices,
            "occupancy_timestamp": format_ist_datetime(get_ist_now())
        }
    
    def get_device_visit_analytics(self, device_id: str) -> Dict[str, Any]:
        """Get visit analytics for a device with daily reset check"""
        # Check for daily reset before calculating analytics
        self.check_and_reset_daily_counters()
        
        current_anchor = self.device_locations.get(device_id)
        visit_start = self.device_visit_times.get(device_id)
        trac_data = self.trac_cache.get(device_id)
        trac_timestamp = None
        if trac_data and hasattr(trac_data, 'timestamp'):
            trac_timestamp = trac_data.timestamp
        
        now = get_ist_now()
        duration_minutes = 0
        if visit_start:
            duration_minutes = int((now - visit_start).total_seconds() / 60)
        
        # Overdue/stale logic for Arrived
        arrived_overdue = False
        arrived_stale = False
        if visit_start:
            # Overdue: Arrived but not moved for > 120 min
            arrived_overdue = duration_minutes > 120
            # Stale: Arrived but not moved for > 240 min
            arrived_stale = duration_minutes > 240
        
        # Overdue/stale logic for Last Track
        last_tracked_overdue = False
        last_tracked_stale = False
        last_tracked_minutes = 0
        if trac_timestamp:
            try:
                last_tracked_dt = parse_datetime(trac_timestamp)
                last_tracked_minutes = int((now - last_tracked_dt).total_seconds() / 60)
                last_tracked_overdue = last_tracked_minutes > 120
                last_tracked_stale = last_tracked_minutes > 240
            except Exception:
                pass
        
        return {
            "current_anchor_id": current_anchor,
            "location_visit_duration": duration_minutes,
            "daily_location_visits": self.daily_visits.get(device_id, 0),
            "visit_start_time": format_ist_datetime(visit_start) if visit_start else None,
            "arrived_overdue": arrived_overdue,
            "arrived_stale": arrived_stale,
            "last_tracked_overdue": last_tracked_overdue,
            "last_tracked_stale": last_tracked_stale,
            "last_tracked_minutes": last_tracked_minutes,
            "last_tracked_time": trac_timestamp
        }
    
    def get_location_traffic_rank(self, anchor_id: str) -> int:
        """Get traffic ranking for location (1=busiest)"""
        occupancy_counts = [(aid, len(devices)) for aid, devices in self.location_occupancy.items()]
        occupancy_counts.sort(key=lambda x: x[1], reverse=True)
        
        for rank, (aid, count) in enumerate(occupancy_counts, 1):
            if aid == anchor_id:
                return rank
        return len(occupancy_counts) + 1
    
    def check_and_reset_daily_counters(self):
        """Check if daily reset is needed and perform it, using persisted ThingsBoard reset date."""
        current_date = get_ist_now().strftime("%Y-%m-%d")
        # Read persisted reset date from ThingsBoard for each device and asset
        persisted_reset_dates = {}
        for device_id, device_config in self.devices.items():
            tb_uuid = getattr(device_config, 'tb_uuid', None)
            if tb_uuid:
                url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/DEVICE/{tb_uuid}/values/attributes/SERVER_SCOPE"
                response = requests.get(url, headers=self.tb_client.get_tb_headers())
                if response.status_code == 200:
                    attr_list = response.json()
                    attrs = {attr["key"]: attr["value"] for attr in attr_list}
                    persisted_reset_dates[device_id] = attrs.get("device_reset_date")
        # Only reset if not already done today
        if self.last_reset_date != current_date and all((persisted_reset_dates.get(did) != current_date for did in self.devices)):
            logging.info(f"[Analytics] 🔄 Daily reset triggered for date: {current_date}")
            self._store_daily_summary()
            total_visits = sum(self.daily_visits.values())
            total_location_visits = sum(self.location_daily_visits.values())
            self.daily_visits.clear()
            self.location_daily_visits.clear()
            self.location_peak_occupancy.clear()
            self.last_reset_date = current_date
            # Persist reset date to ThingsBoard for each device
            for device_id, device_config in self.devices.items():
                tb_uuid = getattr(device_config, 'tb_uuid', None)
                if tb_uuid:
                    url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/DEVICE/{tb_uuid}/attributes"
                    payload = {"device_reset_date": current_date}
                    headers = self.tb_client.get_tb_headers()
                    try:
                        requests.post(url, json=payload, headers=headers)
                    except Exception as e:
                        logging.error(f"[Analytics] Error persisting reset date for device {device_id}: {e}")
            logging.info(f"[Analytics] ✅ Daily counters reset - Previous day: {total_visits} device visits, {total_location_visits} location visits")
        else:
            logging.info(f"[Analytics] Daily reset already performed for date: {current_date}")
    
    def _store_daily_summary(self):
        """Store daily analytics summary before reset"""
        if not self.daily_visits:
            return
            
        total_visits = sum(self.daily_visits.values())
        most_active = max(self.daily_visits.items(), key=lambda x: x[1]) if self.daily_visits else ("None", 0)
        most_popular_location = max(self.location_daily_visits.items(), key=lambda x: x[1]) if self.location_daily_visits else ("None", 0)
        
        logging.info(f"[Analytics] 📊 Daily Summary - Total visits: {total_visits}, Most active device: {most_active[0]} ({most_active[1]} visits)")
        logging.info(f"[Analytics] 📊 Most popular location: {most_popular_location[0]} ({most_popular_location[1]} visits)")
    
    def calculate_utilization_metrics(self, device_id: str, motion_state: str, device_type: str) -> Dict[str, Any]:
        """Calculate comprehensive utilization metrics for a device"""
        visit_analytics = self.get_device_visit_analytics(device_id)
        duration_minutes = visit_analytics.get("location_visit_duration", 0)
        daily_visits = visit_analytics.get("daily_location_visits", 0)
        
        # Calculate usage state from motion and duration
        usage_state = self._determine_usage_state(motion_state, duration_minutes, device_type)
        
        # Calculate daily usage hours (estimated from visits and current session)
        daily_usage_hours = self._calculate_daily_usage_hours(device_id, daily_visits, duration_minutes)
        
        # Calculate utilization rate based on device type
        available_hours = self._get_device_available_hours(device_type)
        utilization_rate = (daily_usage_hours / available_hours) * 100 if available_hours > 0 else 0
        
        # Calculate stagnation metrics
        days_at_location = duration_minutes / (24 * 60)  # Convert to days
        stagnation_level = self._get_stagnation_alert_level(days_at_location, device_type)
        
        # Calculate efficiency score
        efficiency_score = self._calculate_efficiency_score(utilization_rate, days_at_location, daily_visits)
        
        return {
            "current_usage_state": usage_state,
            "daily_usage_hours": round(daily_usage_hours, 2),
            "utilization_rate": round(utilization_rate, 1),
            "days_at_current_location": int(days_at_location),
            "stagnation_alert_level": stagnation_level,
            "hours_since_last_movement": round(duration_minutes / 60, 1),
            "usage_efficiency_score": efficiency_score
        }
    
    def _determine_usage_state(self, motion_state: str, duration_minutes: int, device_type: str) -> str:
        """Determine current usage state from motion and duration"""
        if motion_state == "moving":
            return "in_use"
        elif motion_state == "stationary":
            # Use device-type specific thresholds
            idle_threshold = self._get_idle_threshold_minutes(device_type)
            stagnant_threshold = idle_threshold * 8  # 8x idle threshold for stagnant
            
            if duration_minutes < idle_threshold:
                return "in_use"  # Recently active
            elif duration_minutes < stagnant_threshold:
                return "idle"
            else:
                return "stagnant"
        else:
            return "unknown"
    
    def _get_idle_threshold_minutes(self, device_type: str) -> int:
        """Get idle threshold based on device type"""
        device_lower = device_type.lower()
        if any(t in device_lower for t in ["wheelchair", "bed", "stretcher"]):
            return 60  # 1 hour for mobile patient equipment
        elif any(t in device_lower for t in ["monitor", "pump", "ventilator"]):
            return 120  # 2 hours for medical devices
        elif any(t in device_lower for t in ["tablet", "phone", "tracker"]):
            return 30  # 30 minutes for mobile devices
        else:
            return 90  # Default 1.5 hours
    
    def _calculate_daily_usage_hours(self, device_id: str, daily_visits: int, current_duration: int) -> float:
        """Estimate daily usage hours from visit patterns"""
        if daily_visits == 0:
            return 0
        elif daily_visits == 1:
            # Only current session
            return current_duration / 60
        else:
            # Estimate: current session + estimated previous sessions
            avg_session_duration = 45  # Assume 45 minutes average per session
            estimated_previous_usage = (daily_visits - 1) * avg_session_duration
            total_usage_minutes = current_duration + estimated_previous_usage
            return total_usage_minutes / 60
    
    def _get_device_available_hours(self, device_type: str) -> float:
        """Get available hours per day based on device type"""
        device_lower = device_type.lower()
        if any(t in device_lower for t in ["wheelchair", "bed", "stretcher"]):
            return 16  # 16 hours available (exclude night hours)
        elif any(t in device_lower for t in ["monitor", "pump", "ventilator"]):
            return 20  # 20 hours available (minimal downtime)
        else:
            return 18  # Default 18 hours available
    
    def _get_stagnation_alert_level(self, days_at_location: float, device_type: str) -> str:
        """Get stagnation alert level based on days and device type"""
        device_lower = device_type.lower()
        
        if any(t in device_lower for t in ["wheelchair", "bed", "stretcher"]):
            # High-mobility equipment - stricter thresholds
            if days_at_location >= 5:
                return "critical"
            elif days_at_location >= 2:
                return "warning"
            elif days_at_location >= 1:
                return "attention"
        elif any(t in device_lower for t in ["monitor", "pump", "ventilator"]):
            # Medical devices - moderate thresholds
            if days_at_location >= 10:
                return "critical"
            elif days_at_location >= 5:
                return "warning"
            elif days_at_location >= 2:
                return "attention"
        else:
            # General equipment - relaxed thresholds
            if days_at_location >= 14:
                return "critical"
            elif days_at_location >= 7:
                return "warning"
            elif days_at_location >= 3:
                return "attention"
        
        return "normal"
    
    def _calculate_efficiency_score(self, utilization_rate: float, days_at_location: float, daily_visits: int) -> str:
        """Calculate overall efficiency score"""
        # Penalize low utilization and high stagnation
        efficiency_points = 0
        
        # Utilization component (0-50 points)
        if utilization_rate >= 70:
            efficiency_points += 50
        elif utilization_rate >= 50:
            efficiency_points += 35
        elif utilization_rate >= 30:
            efficiency_points += 20
        else:
            efficiency_points += 10
        
        # Mobility component (0-30 points)
        if daily_visits >= 5:
            efficiency_points += 30
        elif daily_visits >= 3:
            efficiency_points += 20
        elif daily_visits >= 1:
            efficiency_points += 10
        
        # Stagnation penalty (0-20 points)
        if days_at_location < 1:
            efficiency_points += 20
        elif days_at_location < 3:
            efficiency_points += 15
        elif days_at_location < 7:
            efficiency_points += 10
        else:
            efficiency_points += 0
        
        # Convert to grade
        if efficiency_points >= 80:
            return "excellent"
        elif efficiency_points >= 60:
            return "good"
        elif efficiency_points >= 40:
            return "fair"
        else:
            return "poor"
    
    def get_enhanced_location_analytics(self, anchor_id: str) -> Dict[str, Any]:
        """Get comprehensive location analytics including visit density"""
        # Current occupancy data
        occupancy_data = self.get_location_occupancy(anchor_id)
        current_occupancy = occupancy_data["current_device_count"]
        
        # Visit density metrics
        daily_total_visits = self.location_daily_visits.get(anchor_id, 0)
        peak_occupancy = self.location_peak_occupancy.get(anchor_id, 0)
        
        # Calculate visit frequency (visits per hour)
        current_hour = get_ist_now().hour
        visits_per_hour = daily_total_visits / max(current_hour, 1) if current_hour > 0 else daily_total_visits
        
        # Calculate location popularity score
        popularity_score = self._calculate_location_popularity(daily_total_visits, peak_occupancy, current_occupancy)
        
        # Get visit density ranking
        visit_density_rank = self._get_visit_density_ranking(anchor_id)
        
        return {
            # Existing metrics
            "current_occupancy": current_occupancy,
            "devices_present": occupancy_data["devices_present"],
            "occupancy_percentage": min(100, current_occupancy * 10),
            
            # Enhanced visit density metrics
            "daily_total_visits": daily_total_visits,
            "visits_per_hour": round(visits_per_hour, 1),
            "peak_occupancy_today": peak_occupancy,
            "location_popularity_score": round(popularity_score, 1),
            "visit_density_ranking": visit_density_rank,
            
            # Asset demand insights
            "asset_demand_level": self._assess_asset_demand(visits_per_hour, current_occupancy),
            "optimal_asset_count": self._calculate_optimal_assets(daily_total_visits, peak_occupancy)
        }
    
    def _calculate_location_popularity(self, daily_visits: int, peak_occupancy: int, current_occupancy: int) -> float:
        """Calculate location popularity score (0-10)"""
        visit_score = min(5, daily_visits / 5)  # Up to 5 points for visits
        occupancy_score = min(3, peak_occupancy / 3)  # Up to 3 points for peak occupancy
        activity_score = min(2, current_occupancy / 2)  # Up to 2 points for current activity
        return visit_score + occupancy_score + activity_score
    
    def _get_visit_density_ranking(self, anchor_id: str) -> int:
        """Get ranking based on visit density (1 = highest visits)"""
        visit_counts = [(aid, visits) for aid, visits in self.location_daily_visits.items()]
        visit_counts.sort(key=lambda x: x[1], reverse=True)
        
        for rank, (aid, _) in enumerate(visit_counts, 1):
            if aid == anchor_id:
                return rank
        return len(visit_counts) + 1
    
    def _assess_asset_demand(self, visits_per_hour: float, current_occupancy: int) -> str:
        """Assess asset demand level for location"""
        if visits_per_hour >= 3 and current_occupancy <= 2:
            return "high"  # High visits but low assets
        elif visits_per_hour >= 2:
            return "medium"
        elif visits_per_hour >= 1:
            return "low"
        else:
            return "minimal"
    
    def _calculate_optimal_assets(self, daily_visits: int, peak_occupancy: int) -> int:
        """Calculate optimal number of assets for location"""
        # Base on peak occupancy + buffer for high visit locations
        base_assets = peak_occupancy
        visit_buffer = 1 if daily_visits > 10 else 0
        return max(1, base_assets + visit_buffer)
    
    def check_and_reset_daily_counters(self):
        """Check if daily reset is needed and perform it, using persisted ThingsBoard reset date."""
        current_date = get_ist_now().strftime("%Y-%m-%d")
        # Read persisted reset date from ThingsBoard for each device
        persisted_reset_dates = {}
        for device_id, device_config in self.devices.items():
            tb_uuid = getattr(device_config, 'tb_uuid', None)
            if tb_uuid:
                url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/DEVICE/{tb_uuid}/values/attributes/SERVER_SCOPE"
                response = requests.get(url, headers=self.tb_client.get_tb_headers())
                if response.status_code == 200:
                    attr_list = response.json()
                    attrs = {attr["key"]: attr["value"] for attr in attr_list}
                    persisted_reset_dates[device_id] = attrs.get("device_reset_date")

        if self.last_reset_date != current_date and all((persisted_reset_dates.get(did) != current_date for did in self.devices)):
            logging.info(f"[Analytics] 🔄 Daily reset triggered for date: {current_date}")

            self._store_daily_summary()

            total_visits = sum(self.daily_visits.values())
            total_location_visits = sum(self.location_daily_visits.values())

            self.daily_visits.clear()
            self.location_daily_visits.clear()
            self.location_peak_occupancy.clear()

            self.last_reset_date = current_date

            # Persist reset date to ThingsBoard for each device
            for device_id, device_config in self.devices.items():
                tb_uuid = getattr(device_config, 'tb_uuid', None)
                if tb_uuid:
                    url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/DEVICE/{tb_uuid}/attributes"
                    payload = {"device_reset_date": current_date}
                    headers = self.tb_client.get_tb_headers()
                    try:
                        requests.post(url, json=payload, headers=headers)
                    except Exception as e:
                        logging.error(f"[Analytics] Error persisting reset date for device {device_id}: {e}")

            logging.info(f"[Analytics] ✅ Daily counters reset - Previous day: {total_visits} device visits, {total_location_visits} location visits")
        else:
            logging.info(f"[Analytics] Daily reset already performed for date: {current_date}")
    
    def _store_daily_summary(self):
        """Store daily analytics summary before reset"""
        if not self.daily_visits:
            return
            
        total_visits = sum(self.daily_visits.values())
        most_active = max(self.daily_visits.items(), key=lambda x: x[1]) if self.daily_visits else ("None", 0)
        most_popular_location = max(self.location_daily_visits.items(), key=lambda x: x[1]) if self.location_daily_visits else ("None", 0)
        
        logging.info(f"[Analytics] 📊 Daily Summary - Total visits: {total_visits}, Most active device: {most_active[0]} ({most_active[1]} visits)")
        logging.info(f"[Analytics] 📊 Most popular location: {most_popular_location[0]} ({most_popular_location[1]} visits)")
    
    def export_devices_to_csv(self, filename: str = "devices_export.csv") -> bool:
        """Export all cached devices to CSV file"""
        try:
            import csv
            import os
            from datetime import datetime
            
            if not self.devices:
                logging.warning("[Export] No devices loaded to export")
                return False
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"devices_export_{timestamp}.csv"
            
            # Define CSV columns (excluding tb_uuid, tb_token)
            columns = ["device_id", "device_type", "name", "dept", "device_profile", "fencing_aps"]
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter='|')
                
                # Write header
                writer.writerow(columns)
                
                # Write device data
                for device_id, device_config in self.devices.items():
                    row = [
                        device_config.device_id,
                        device_config.device_type or "",
                        device_config.name or "",
                        device_config.dept or "",
                        device_config.device_profile or "",
                        device_config.fencing_aps or ""
                    ]
                    writer.writerow(row)
            
            logging.info(f"[Export] ✅ Devices exported to: {filename}")
            logging.info(f"[Export] 📊 Exported {len(self.devices)} devices")
            return True
            
        except Exception as e:
            logging.error(f"[Export] ❌ Device export error: {e}")
            return False
    
    def export_assets_to_csv(self, filename: str = "assets_export.csv") -> bool:
        """Export all cached assets to CSV file"""
        try:
            import csv
            import os
            from datetime import datetime
            
            if not self.assets:
                logging.warning("[Export] No assets loaded to export")
                return False
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"assets_export_{timestamp}.csv"
            
            # Define CSV columns
            columns = ["deviceid", "label", "asset_type", "dept", "aposX", "aposY", 
                      "posX", "posY", "lat", "lng", "aimgCord", "fimgCord", "gpsCord"]
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile, delimiter='|')
                
                # Write header
                writer.writerow(columns)
                
                # Write asset data
                for asset_id, asset_config in self.assets.items():
                    row = [
                        asset_config.asset_id,
                        asset_config.label or "",
                        "ANCHOR",  # Default asset type
                        asset_config.dept or "",
                        asset_config.apos_x,
                        asset_config.apos_y,
                        asset_config.pos_x,
                        asset_config.pos_y,
                        asset_config.lat,
                        asset_config.lng,
                        asset_config.gps_cord or "",  # Will contain aimgCord JSON
                        asset_config.img_cord or "",  # Will contain fimgCord JSON  
                        asset_config.gps_cord or ""   # Will contain gpsCord JSON
                    ]
                    writer.writerow(row)
            
            logging.info(f"[Export] ✅ Assets exported to: {filename}")
            logging.info(f"[Export] 📊 Exported {len(self.assets)} assets")
            return True
            
        except Exception as e:
            logging.error(f"[Export] ❌ Asset export error: {e}")
            return False

    def send_daily_reset_telemetry(self):
        """Send daily reset telemetry for all devices to ThingsBoard"""
        logging.info("[Reset] 🔄 Sending daily reset telemetry for all devices...")
        for device_id, device_config in self.devices.items():
            if not hasattr(device_config, 'tb_token') or not device_config.tb_token:
                continue
            current_date = get_ist_now().strftime("%Y-%m-%d")
            reset_payload = {
                "ts": int(time.time() * 1000),
                "values": {
                    "daily_location_visits": 0,
                    "total_visits": 0,
                    "location_visit_duration": 0,
                    "utilization_rate": 0,
                    "usage_state": "reset",
                    "daily_usage_hours": 0,
                    "days_at_location": 0,
                    "arrived_overdue": False,
                    "arrived_stale": False,
                    "last_tracked_overdue": False,
                    "last_tracked_stale": False,
                    "device_reset_date": current_date
                }
            }
            url = f"https://assettracking.stjohns.in/api/v1/{device_config.tb_token}/telemetry"
            headers = {"Content-Type": "application/json"}
            try:
                response = requests.post(url, json=reset_payload, headers=headers)
                if response.status_code == 200:
                    logging.info(f"[Reset] ✅ Reset telemetry sent for device {device_id}")
                else:
                    logging.error(f"[Reset] ❌ Failed to send reset telemetry for device {device_id}: {response.status_code} - {response.text}")
            except Exception as e:
                logging.error(f"[Reset] ❌ Exception sending reset telemetry for device {device_id}: {e}")

    def send_daily_reset_asset_telemetry(self):
        """Send daily reset telemetry for all assets/anchors to ThingsBoard, including unknown anchors, and persist reset date."""
        logging.info("[Reset] 🔄 Sending daily reset telemetry for all assets/anchors (including unknown)...")
        current_date = get_ist_now().strftime("%Y-%m-%d")
        # Reset for known assets only if not already reset today
        persisted_asset_reset_dates = {}
        for anchor_id, asset_config in self.assets.items():
            if not hasattr(asset_config, 'tb_uuid') or not asset_config.tb_uuid:
                continue
            # Read persisted asset_reset_date
            attr_url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/ASSET/{asset_config.tb_uuid}/values/attributes/SERVER_SCOPE"
            attr_response = requests.get(attr_url, headers=self.tb_client.get_tb_headers())
            if attr_response.status_code == 200:
                attr_list = attr_response.json()
                attrs = {attr["key"]: attr["value"] for attr in attr_list}
                persisted_asset_reset_dates[anchor_id] = attrs.get("asset_reset_date")
            else:
                persisted_asset_reset_dates[anchor_id] = None
        for anchor_id, asset_config in self.assets.items():
            if not hasattr(asset_config, 'tb_uuid') or not asset_config.tb_uuid:
                continue
            if persisted_asset_reset_dates.get(anchor_id) == current_date:
                logging.info(f"[Reset] Asset {anchor_id} already reset for date: {current_date}")
                continue
            reset_payload = {
                "anchor_id": anchor_id,
                "location_name": getattr(asset_config, 'label', f"Anchor {anchor_id}"),
                "location_type": "reset",
                "department": asset_config.dept,
                "current_device_count": 0,
                "devices_present": "",
                "occupancy_percentage": 0,
                "traffic_ranking": 0,
                "daily_total_visits": 0,
                "visits_per_hour": 0,
                "peak_occupancy_today": 0,
                "location_popularity_score": 0,
                "visit_density_ranking": 0,
                "asset_demand_level": "reset",
                "optimal_asset_count": 0,
                "latitude": getattr(asset_config, 'lat', 0),
                "longitude": getattr(asset_config, 'lng', 0),
                "apos_x": getattr(asset_config, 'apos_x', 0),
                "apos_y": getattr(asset_config, 'apos_y', 0),
                "last_updated": format_ist_datetime(get_ist_now()),
                "arrived_overdue": False,
                "arrived_stale": False,
                "last_tracked_overdue": False,
                "last_tracked_stale": False,
                "asset_reset_date": current_date
            }
            url = f"https://assettracking.stjohns.in/api/plugins/telemetry/ASSET/{asset_config.tb_uuid}/timeseries/ts"
            headers = {
                "Content-Type": "application/json",
                "X-Authorization": f"Bearer {self.tb_client.thingsboard_jwt}"
            }
            try:
                response = requests.post(url, json=reset_payload, headers=headers)
                if response.status_code == 200:
                    logging.info(f"[Reset] ✅ Reset asset telemetry sent for anchor {anchor_id}")
                    # Persist asset reset date as attribute
                    attr_url = f"http://{self.tb_client.config['THINGSBOARD_HOST']}/api/plugins/telemetry/ASSET/{asset_config.tb_uuid}/attributes"
                    attr_payload = {"asset_reset_date": current_date}
                    attr_headers = self.tb_client.get_tb_headers()
                    try:
                        requests.post(attr_url, json=attr_payload, headers=attr_headers)
                    except Exception as e:
                        logging.error(f"[Reset] Error persisting reset date for asset {anchor_id}: {e}")
                else:
                    logging.error(f"[Reset] ❌ Failed to send reset asset telemetry for anchor {anchor_id}: {response.status_code} - {response.text}")
            except Exception as e:
                logging.error(f"[Reset] ❌ Exception sending reset asset telemetry for anchor {anchor_id}: {e}")

        # Reset for unknown anchors (anchors in occupancy but not in assets)
        unknown_anchors = set(self.location_occupancy.keys()) - set(self.assets.keys())
        for anchor_id in unknown_anchors:
            logging.info(f"[Reset] 🔄 Sending reset telemetry for unknown anchor {anchor_id}")
            reset_payload = {
                "anchor_id": anchor_id,
                "location_name": f"Unknown Location (Anchor: {anchor_id})",
                "location_type": "reset",
                "department": "Unknown",
                "current_device_count": 0,
                "devices_present": "",
                "occupancy_percentage": 0,
                "traffic_ranking": 0,
                "daily_total_visits": 0,
                "visits_per_hour": 0,
                "peak_occupancy_today": 0,
                "location_popularity_score": 0,
                "visit_density_ranking": 0,
                "asset_demand_level": "reset",
                "optimal_asset_count": 0,
                "latitude": 0,
                "longitude": 0,
                "apos_x": 0,
                "apos_y": 0,
                "last_updated": format_ist_datetime(get_ist_now()),
                "arrived_overdue": False,
                "arrived_stale": False,
                "last_tracked_overdue": False,
                "last_tracked_stale": False
            }
            # For unknown anchors, we may not have a tb_uuid, so skip if not available
            # If you have a way to get tb_uuid for unknown anchors, add it here
            # Otherwise, log and skip actual POST
            logging.warning(f"[Reset] Unknown anchor {anchor_id} has no tb_uuid; telemetry reset payload prepared but not sent.")

        def run_periodic_overdue_check(self):
            """Periodically check and update overdue/stale flags for all devices, sending telemetry if needed."""
            logging.info("[PeriodicCheck] Running overdue/stale flag check for all devices...")
            for device_id, device_config in self.devices.items():
                analytics = self.get_device_visit_analytics(device_id)
                # Only send telemetry if overdue/stale flags are True
                if any([
                    analytics.get("arrived_overdue"),
                    analytics.get("arrived_stale"),
                    analytics.get("last_tracked_overdue"),
                    analytics.get("last_tracked_stale")
                ]):
                    if hasattr(device_config, 'tb_token') and device_config.tb_token:
                        payload = {
                            "ts": int(time.time() * 1000),
                            "values": {
                                "arrived_overdue": analytics.get("arrived_overdue"),
                                "arrived_stale": analytics.get("arrived_stale"),
                                "last_tracked_overdue": analytics.get("last_tracked_overdue"),
                                "last_tracked_stale": analytics.get("last_tracked_stale")
                            }
                        }
                        url = f"https://assettracking.stjohns.in/api/v1/{device_config.tb_token}/telemetry"
                        headers = {"Content-Type": "application/json"}
                        try:
                            response = requests.post(url, json=payload, headers=headers)
                            if response.status_code == 200:
                                logging.info(f"[PeriodicCheck] Telemetry sent for device {device_id}")
                            else:
                                logging.error(f"[PeriodicCheck] Failed telemetry for device {device_id}: {response.status_code} - {response.text}")
                        except Exception as e:
                            logging.error(f"[PeriodicCheck] Exception sending telemetry for device {device_id}: {e}")
            logging.info("[PeriodicCheck] Overdue/stale flag check complete.")
# Periodic scheduler loop for overdue/stale flag check
if __name__ == "__main__":
    import time
    import threading
    # Instantiate CacheManager as needed (example: cache_manager = CacheManager(tb_client))
    # You must ensure tb_client is initialized before this
    # cache_manager = CacheManager(tb_client)

    def periodic_task():
        while True:
            try:
                cache_mgr = globals().get("cache_manager")
                if cache_mgr is not None:
                    cache_mgr.run_periodic_overdue_check()
                else:
                    logging.debug("[PeriodicCheck] cache_manager not initialized; skipping periodic run")
            except Exception as e:
                logging.error(f"[PeriodicCheck] Exception in periodic task: {e}")
            time.sleep(600)  # Run every 10 minutes

    # Start the periodic task in a background thread
    # threading.Thread(target=periodic_task, daemon=True).start()
    # Optionally, keep main thread alive
    # while True:
    #     time.sleep(3600)


class CoordinateGenerator:
    """Handle multi-level coordinate generation from Asset configurations"""
    
    @staticmethod
    def generate_coordinates(asset_config: Optional[AssetConfig]) -> Dict[str, float]:
        """Generate all coordinate levels from asset configuration"""
        if not asset_config:
            # Default coordinates for Christ University
            return {
                "latitude": 12.9310000,
                "longitude": 77.6194000,
                "apos_x": 0.73,
                "apos_y": 0.45,
                "pos_x": 0.0,
                "pos_y": 0.0
            }
        
        return {
            "latitude": asset_config.lat,
            "longitude": asset_config.lng,
            "apos_x": asset_config.apos_x,  # Building coordinates
            "apos_y": asset_config.apos_y,
            "pos_x": asset_config.pos_x,    # Floor coordinates
            "pos_y": asset_config.pos_y
        }


class FenceValidator:
    """Handle fence violation detection with corrected logic"""
    
    @staticmethod
    def check_fence_violation(device_config: Optional[DeviceConfig], current_ap: str) -> bool:
        """
        Check if device is violating fence boundaries
        Returns True if VIOLATING (outside allowed APs)
        Returns False if OK (inside allowed APs)
        """
        if not device_config or not device_config.fencing_aps:
            # No fencing configured - assume OK
            return False
            
        # Parse allowed APs
        allowed_aps_str = device_config.fencing_aps.strip('"')
        allowed_aps = [ap.strip() for ap in allowed_aps_str.split(",") if ap.strip()]
        
        # CORRECTED LOGIC: violation when NOT in allowed list
        is_violation = current_ap not in allowed_aps
        
        logging.debug(f"[Fence] Device {device_config.device_id}: AP {current_ap}, "
                     f"Allowed: {allowed_aps}, Violation: {is_violation}")
        
        return is_violation


class ThingsboardKinesisBridge:
    """Refactored IoT bridge with proper architecture and caching"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.thingsboard_jwt = ""
        self.kinesis_access_token = ""
        
        # Initialize cache manager and utilities
        self.cache_manager = CacheManager(self)
        self.coordinate_generator = CoordinateGenerator()
        self.fence_validator = FenceValidator()
        
        # WebSocket reconnection management
        self.reconnect_delay = 5
        
        # Current logging level
        self.current_log_level = self._get_log_level_from_config()
        
        # Configure logging
        self._configure_logging()
        
    def _get_log_level_from_config(self) -> int:
        """Get logging level from configuration"""
        log_level_str = self.config.get('LOG_LEVEL', 'INFO').upper()
        
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO, 
            'WARNING': logging.WARNING,
            'WARN': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        
        return level_mapping.get(log_level_str, logging.INFO)
    
    def _configure_logging(self):
        """Configure advanced logging with dynamic control"""
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            
        # Configure format based on log level
        if self.current_log_level == logging.DEBUG:
            log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        else:
            log_format = '%(asctime)s - %(levelname)s - %(message)s'
            
        logging.basicConfig(
            level=self.current_log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler()
            ]
        )
        
        # Set specific logger levels for external libraries
        if self.current_log_level > logging.DEBUG:
            logging.getLogger('websocket').setLevel(logging.WARNING)
            logging.getLogger('urllib3').setLevel(logging.WARNING)
            logging.getLogger('requests').setLevel(logging.WARNING)
        
        logging.info(f"[Config] Logging configured at {logging.getLevelName(self.current_log_level)} level")
    
    def set_log_level(self, level: str):
        """Dynamically change log level during runtime"""
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING, 
            'WARN': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        
        new_level = level_mapping.get(level.upper())
        if new_level is not None:
            self.current_log_level = new_level
            logging.getLogger().setLevel(new_level)
            logging.info(f"[Config] Log level changed to {level.upper()}")
            return True
        else:
            logging.error(f"[Config] Invalid log level: {level}. Valid levels: {list(level_mapping.keys())}")
            return False
    
    def get_current_log_level(self) -> str:
        """Get current logging level as string"""
        return logging.getLevelName(self.current_log_level)
    
    def enable_debug_mode(self):
        """Quick switch to debug mode"""
        self.set_log_level('DEBUG')
        
    def enable_quiet_mode(self):
        """Quick switch to warning/error only"""
        self.set_log_level('WARNING')
        
    def enable_normal_mode(self):
        """Quick switch to info mode"""
        self.set_log_level('INFO')
    


    # =========================================================================
    # THINGSBOARD AUTHENTICATION
    # =========================================================================
    
    def authenticate_thingsboard(self) -> bool:
        """Authenticate with ThingsBoard and get JWT token"""
        try:
            url = f"http://{self.config['THINGSBOARD_HOST']}/api/auth/login"
            payload = {
                "username": self.config['THINGSBOARD_USERNAME'],
                "password": self.config['THINGSBOARD_PASSWORD']
            }
            headers = {"Content-Type": "application/json"}
            
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code == 200:
                self.thingsboard_jwt = response.json().get("token")
                logging.info("[TB] ✅ Authentication successful")
                return True
            else:
                logging.error(f"[TB] ❌ Authentication failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"[TB] ❌ Authentication error: {e}")
            return False
    
    def get_tb_headers(self) -> Dict[str, str]:
        """Get ThingsBoard API headers with authentication"""
        if not self.thingsboard_jwt:
            self.authenticate_thingsboard()
            
        return {
            "Content-Type": "application/json",
            "X-Authorization": f"Bearer {self.thingsboard_jwt}"
        }
    
    # =========================================================================
    # KINESIS AUTHENTICATION
    # =========================================================================
    
    def authenticate_kinesis(self) -> bool:
        """Authenticate with Kinesis and get access token"""
        try:
            oauth_url = "https://auth.atollkinesis.com/token/auth/client"
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.config['CLIENT_ID'],
                "client_secret": self.config['CLIENT_SECRET'],
                "scope": "kinesis"
            }
            headers = {"Content-Type": "application/json"}
            
            response = requests.post(oauth_url, json=payload, headers=headers)
            
            if response.status_code == 200:
                self.kinesis_access_token = response.json().get("access_token")
                logging.info("[Kinesis] ✅ Authentication successful")
                return True
            else:
                logging.error(f"[Kinesis] ❌ Authentication failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"[Kinesis] ❌ Authentication error: {e}")
            return False
    
    # =========================================================================
    # TELEMETRY PROCESSING
    # =========================================================================
    
    def send_telemetry_to_thingsboard(self, device_id: str, telemetry: Dict[str, Any]) -> bool:
        """Send telemetry data to ThingsBoard with proper validation and timeseries format"""
        try:
            device_config = self.cache_manager.get_device_config(device_id)
            
            if not device_config:
                logging.warning(f"[TB] Device {device_id} not in allowed cache, skipping telemetry")
                return False
                
            if not device_config.tb_token:
                logging.warning(f"[TB] No token for device {device_id}, skipping telemetry")
                return False
            
            # Convert to ThingsBoard timeseries format
            timestamp = telemetry.get("timestamp")
            ts_value = None
            
            if timestamp:
                try:
                    # Parse ISO timestamp to milliseconds
                    from datetime import datetime
                    if isinstance(timestamp, str):
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        ts_value = int(dt.timestamp() * 1000)
                    elif isinstance(timestamp, (int, float)):
                        ts_value = int(timestamp * 1000) if timestamp < 1e10 else int(timestamp)
                except Exception as e:
                    logging.warning(f"[TB] Could not parse timestamp for {device_id}: {e}")
            
            # If no valid timestamp, use current time with microseconds for uniqueness
            if not ts_value:
                import time
                ts_value = int(time.time() * 1000) + (int(time.time() * 1000000) % 1000)
            
            # Ensure timestamp uniqueness per device (add small increment if needed)
            if not hasattr(self, '_last_timestamps'):
                self._last_timestamps = {}
            
            if device_id in self._last_timestamps and ts_value <= self._last_timestamps[device_id]:
                ts_value = self._last_timestamps[device_id] + 1
            
            self._last_timestamps[device_id] = ts_value
            
            # Extract values (exclude timestamp and device_id from values)
            values = {k: v for k, v in telemetry.items() 
                     if k not in ['timestamp'] and v is not None}
            
            # Build ThingsBoard timeseries payload
            tb_payload = {
                "ts": ts_value,
                "values": values
            }
            
            url = f"https://assettracking.stjohns.in/api/v1/{device_config.tb_token}/telemetry"
            headers = {"Content-Type": "application/json"}
            
            logging.debug(f"[TB] 🌐 Device telemetry URL: {url}")
            response = requests.post(url, json=tb_payload, headers=headers)
            
            if response.status_code == 200:
                payload_size = len(json.dumps(tb_payload))
                logging.info(f"[TB] ✅ Telemetry sent for {device_id}: Payload: {tb_payload} Size: {payload_size} bytes")
                return True
            else:
                logging.error(f"[TB] ❌ Telemetry failed for {device_id}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"[TB] ❌ Telemetry error for {device_id}: {e}")
            return False
    
    def send_asset_telemetry_to_thingsboard(self, anchor_id: str) -> bool:
        """Send location analytics telemetry for an asset/anchor"""
        try:
            asset_config = self.cache_manager.assets.get(anchor_id)
            if not asset_config:
                logging.warning(f"[ASSET] ❌ No asset config found for anchor {anchor_id}")
                return False
            asset_send_success = False
            
            # Get occupancy data
            occupancy_data = self.cache_manager.get_location_occupancy(anchor_id)
            traffic_rank = self.cache_manager.get_location_traffic_rank(anchor_id)
            
            # Calculate analytics
            device_count = occupancy_data["current_device_count"]
            devices_present = occupancy_data["devices_present"]
            
            # Determine location details using enhanced naming logic
            department = getattr(asset_config, 'dept', 'Unknown')
            location_label = getattr(asset_config, 'label', None)
            
            if location_label and department != 'Unknown':
                location_name = f"{location_label} ({department})"
            elif location_label:
                location_name = location_label
            elif department != 'Unknown':
                location_name = f"{department} - Anchor {anchor_id}"
            else:
                location_name = f"Anchor {anchor_id}"
            
            location_type = "Unknown"
            
            if hasattr(asset_config, 'dept') and asset_config.dept:
                dept_lower = asset_config.dept.lower()
                if "corridor" in dept_lower:
                    location_type = "Corridor"
                elif "icu" in dept_lower:
                    location_type = "ICU"
                elif "emergency" in dept_lower:
                    location_type = "Emergency Room"
                elif "ward" in dept_lower or "picu" in dept_lower:
                    location_type = "Ward"
                else:
                    location_type = "Department"
            
            # Get enhanced location analytics
            enhanced_analytics = self.cache_manager.get_enhanced_location_analytics(anchor_id)
            
            # Generate timestamp
            ts_value = int(time.time() * 1000)
            
            # Build asset telemetry payload with enhanced metrics
            tb_payload = {
                "ts": ts_value,
                "values": {
                    "anchor_id": anchor_id,
                    "location_name": location_name,
                    "location_type": location_type,
                    "department": department,
                    
                    # Current occupancy metrics
                    "current_device_count": enhanced_analytics["current_occupancy"],
                    "devices_present": ",".join(enhanced_analytics["devices_present"]),
                    "occupancy_percentage": enhanced_analytics["occupancy_percentage"],
                    "traffic_ranking": traffic_rank,
                    
                    # Enhanced visit density metrics
                    "daily_total_visits": enhanced_analytics["daily_total_visits"],
                    "visits_per_hour": enhanced_analytics["visits_per_hour"],
                    "peak_occupancy_today": enhanced_analytics["peak_occupancy_today"],
                    "location_popularity_score": enhanced_analytics["location_popularity_score"],
                    "visit_density_ranking": enhanced_analytics["visit_density_ranking"],
                    
                    # Asset demand insights
                    "asset_demand_level": enhanced_analytics["asset_demand_level"],
                    "optimal_asset_count": enhanced_analytics["optimal_asset_count"],
                    
                    # Location coordinates
                    "latitude": asset_config.lat,
                    "longitude": asset_config.lng,
                    "apos_x": asset_config.apos_x,
                    "apos_y": asset_config.apos_y,
                    "last_updated": format_ist_datetime(get_ist_now())
                }
            }
            
            # Send asset telemetry to ThingsBoard using proper asset REST API
            if asset_config.tb_uuid:
                # Use enhanced analytics data for official API
                clean_asset_data = {
                    "anchor_id": anchor_id,
                    "location_name": location_name,
                    "location_type": location_type,
                    "department": department,
                    
                    # Current occupancy metrics
                    "current_device_count": enhanced_analytics["current_occupancy"],
                    "devices_present": ",".join(enhanced_analytics["devices_present"]),
                    "occupancy_percentage": enhanced_analytics["occupancy_percentage"],
                    "traffic_ranking": traffic_rank,
                    
                    # Enhanced visit density metrics
                    "daily_total_visits": enhanced_analytics["daily_total_visits"],
                    "visits_per_hour": enhanced_analytics["visits_per_hour"],
                    "peak_occupancy_today": enhanced_analytics["peak_occupancy_today"],
                    "location_popularity_score": enhanced_analytics["location_popularity_score"],
                    "visit_density_ranking": enhanced_analytics["visit_density_ranking"],
                    
                    # Asset demand insights
                    "asset_demand_level": enhanced_analytics["asset_demand_level"],
                    "optimal_asset_count": enhanced_analytics["optimal_asset_count"],
                    
                    # Location coordinates
                    "latitude": asset_config.lat,
                    "longitude": asset_config.lng,
                    "apos_x": asset_config.apos_x,
                    "apos_y": asset_config.apos_y,
                    "last_updated": format_ist_datetime(get_ist_now())
                }
                
                # Add device type analytics if devices are present
                devices_present = enhanced_analytics["devices_present"]
                if devices_present:
                    clean_asset_data["device_types_present"] = ",".join([
                        self.cache_manager.get_device_type_for_analytics(dev) for dev in devices_present[:5]
                    ])
                    clean_asset_data["device_names_present"] = ",".join([
                        (self.cache_manager.get_device_config(dev).label
                         or self.cache_manager.get_device_config(dev).name
                         or dev)
                        for dev in devices_present[:5]
                        if self.cache_manager.get_device_config(dev)
                    ])
                
                # Official ThingsBoard Asset REST API endpoint
                url = f"https://assettracking.stjohns.in/api/plugins/telemetry/ASSET/{asset_config.tb_uuid}/timeseries/ts"
                # Try multiple payload styles for compatibility across ThingsBoard versions.
                payload_candidates = [
                    clean_asset_data,
                    [{"ts": ts_value, "values": clean_asset_data}],
                    {"ts": ts_value, "values": clean_asset_data}
                ]

                logging.info(f"[ASSET] 🌐 Official Asset API URL: {url}")
                response = None
                sent_ok = False
                auth_refresh_used = False

                for idx, candidate_payload in enumerate(payload_candidates, 1):
                    headers = self.get_tb_headers()
                    response = requests.post(url, json=candidate_payload, headers=headers)

                    # Handle expired JWT once per payload attempt.
                    if response.status_code == 401:
                        logging.warning(f"[ASSET] JWT expired while posting asset telemetry for {anchor_id}, re-authenticating...")
                        if self.authenticate_thingsboard():
                            auth_refresh_used = True
                            headers = self.get_tb_headers()
                            response = requests.post(url, json=candidate_payload, headers=headers)

                    if response.status_code == 200:
                        logging.info(f"[ASSET] ✅ Asset telemetry sent via official API for {anchor_id} (payload style #{idx})")
                        logging.debug(f"[ASSET] 📡 Asset data keys: {list(clean_asset_data.keys())}")
                        sent_ok = True
                        break
                    else:
                        logging.warning(
                            f"[ASSET] Payload style #{idx} failed for {anchor_id}: "
                            f"{response.status_code} - {response.text[:200]}"
                        )

                if not sent_ok:
                    status_code = response.status_code if response is not None else "no_response"
                    response_text = response.text[:200] if response is not None else "no_response"
                    logging.error(f"[ASSET] ❌ Official Asset API failed for {anchor_id}: {status_code} - {response_text}")
                else:
                    logging.info(
                        f"[ASSET] ✅ Post summary for {anchor_id}: success=true, auth_refresh_used={auth_refresh_used}"
                    )
                asset_send_success = sent_ok
            else:
                logging.warning(f"[ASSET] No asset UUID available for {anchor_id}, cannot send asset telemetry")
                
                # Fallback to device token method if no asset UUID
                if devices_present and len(devices_present) > 0:
                    logging.info(f"[ASSET] Using device token fallback for asset {anchor_id} without UUID")
                    sample_device = devices_present[0]
                    sample_device_config = self.cache_manager.get_device_config(sample_device)
                    
                    if sample_device_config and sample_device_config.tb_token:
                        fallback_data = {
                            "ts": tb_payload["ts"],
                            "values": {f"asset_no_uuid_{anchor_id}_{k}": v for k, v in tb_payload["values"].items()}
                        }
                        
                        fallback_url = f"https://assettracking.stjohns.in/api/v1/{sample_device_config.tb_token}/telemetry"
                        fallback_headers = {"Content-Type": "application/json"}
                        
                        fallback_response = requests.post(fallback_url, json=fallback_data, headers=fallback_headers)
                        if fallback_response.status_code == 200:
                            logging.info(f"[ASSET] ✅ No-UUID fallback telemetry sent for {anchor_id}")
                            asset_send_success = True
                        else:
                            logging.error(f"[ASSET] ❌ No-UUID fallback failed for {anchor_id}: {fallback_response.status_code}")
            
            # Enhanced analytics logging
            logging.info(f"[ASSET] 📊 Enhanced Analytics for {anchor_id}:")
            logging.info(f"[ASSET] 🏥 Location: {location_name} ({location_type}) - Demand: {enhanced_analytics['asset_demand_level']}")
            logging.info(f"[ASSET] 📈 Visits: {enhanced_analytics['daily_total_visits']} total, {enhanced_analytics['visits_per_hour']}/hr - Rank #{enhanced_analytics['visit_density_ranking']}")
            logging.info(f"[ASSET] 🎯 Occupancy: {enhanced_analytics['current_occupancy']} current, {enhanced_analytics['peak_occupancy_today']} peak - Score: {enhanced_analytics['location_popularity_score']}")
            logging.info(f"[ASSET] 💡 Optimal assets: {enhanced_analytics['optimal_asset_count']} - Current devices: {','.join(enhanced_analytics['devices_present']) if enhanced_analytics['devices_present'] else 'None'}")
            logging.debug(f"[ASSET] 📋 Full enhanced payload keys: {list(clean_asset_data.keys()) if 'clean_asset_data' in locals() else 'N/A'}")
            
            return asset_send_success
            
        except Exception as e:
            logging.error(f"[ASSET] ❌ Analytics error for {anchor_id}: {e}")
            return False
    
    def process_kinesis_message(self, payload: Dict[str, Any]):
        """Process incoming Kinesis WebSocket message with corrected structure"""
        try:
            # Log raw WebSocket payload for debugging
            logging.info(f"[WS] 📡 Raw payload received: {payload}")
            
            event_type = payload.get("hdr", {}).get("evt_type")
            device_entries = payload.get("data", [])
            
            logging.info(f"[WS] Processing {event_type} event with {len(device_entries)} entries")
            
            for entry in device_entries:
                device_id = entry.get("device_id")
                
                if not device_id:
                    logging.warning("[WS] Missing device_id, skipping entry")
                    continue
                    
                if not self.cache_manager.is_device_allowed(device_id):
                    logging.debug(f"[WS] Device {device_id} not in allowed cache, skipping")
                    continue
                
                # Build telemetry data
                telemetry = self._build_telemetry_data(entry, event_type)
                
                # Process positioning data
                self._process_positioning_data(entry, event_type, device_id, telemetry)
                anchor_id = telemetry.get("trac_pos_ref")
                
                # Send device telemetry
                success = self.send_telemetry_to_thingsboard(device_id, telemetry)
                asset_success = None

                # Always attempt asset analytics on trac events (independent of device telemetry result)
                if anchor_id and event_type == "trac":
                    if not success:
                        logging.warning(
                            f"[Analytics] Device telemetry failed for {device_id}; still sending asset analytics for "
                            f"anchor {anchor_id}"
                        )
                    logging.debug(f"[Analytics] 📡 Sending asset analytics for anchor: {anchor_id}")
                    asset_success = self.send_asset_telemetry_to_thingsboard(anchor_id)

                logging.info(
                    f"[FLOW] device={device_id} event={event_type} anchor={anchor_id or '-'} "
                    f"device_telemetry_success={success} asset_telemetry_success={asset_success}"
                )
                
        except Exception as e:
            logging.error(f"[WS] ❌ Message processing error: {e}")
    
    def _build_telemetry_data(self, entry: Dict[str, Any], event_type: str) -> Dict[str, Any]:
        """Build base telemetry data from Kinesis entry with enhanced analytics"""
        device_id = entry.get("device_id")
        
        # Get sensor data based on event type
        if event_type == "tele":
            sensor_data = entry.get("data", {})
        elif event_type == "status":
            sensor_data = entry.get("latest_tele", {})
        else:
            sensor_data = {}
        
        telemetry = {
            "device_id": device_id,
            "event_type": event_type,
            "relayer_id": entry.get("relayer_id"),
            "last_seen": entry.get("last_seen"),
            "status": entry.get("status"),
            "state": entry.get("state"),
            "timestamp": (
                sensor_data.get("timestamp") or 
                entry.get("timestamp") or 
                entry.get("device_ts") or 
                entry.get("server_ts")
            )
        }
        
        # Add sensor data fields
        if sensor_data:
            telemetry.update({
                "motion_state": sensor_data.get("motion_state"),
                "orientation": sensor_data.get("orientation") or sensor_data.get("orient"),
                "event": sensor_data.get("event"),
                "ext_power": sensor_data.get("ext_power")
            })
            
            # Handle numeric fields with validation
            for field, convert_func in [("batt", int), ("light", int)]:
                value = sensor_data.get(field)
                if value is not None:
                    try:
                        telemetry[field] = convert_func(value)
                    except (ValueError, TypeError):
                        telemetry[field] = value
            
            # Handle GNSS coordinates
            self._process_gnss_coordinates(sensor_data, telemetry, device_id)
            
            # Add enhanced analytics if motion state is available
            if sensor_data.get("motion_state") and telemetry.get("device_type"):
                utilization_metrics = self.cache_manager.calculate_utilization_metrics(
                    device_id, 
                    sensor_data.get("motion_state"),
                    telemetry.get("device_type", "Unknown")
                )
                telemetry.update(utilization_metrics)
        
        return telemetry
    
    def _process_gnss_coordinates(self, sensor_data: Dict[str, Any], telemetry: Dict[str, Any], device_id: str):
        """Process GNSS positioning data if available"""
        gnss = sensor_data.get("gnss_pos")
        if gnss:
            try:
                coords = json.loads(gnss) if isinstance(gnss, str) else gnss
                if isinstance(coords, list) and len(coords) == 2:
                    telemetry["gnss_latitude"] = float(coords[0])
                    telemetry["gnss_longitude"] = float(coords[1])
                    logging.debug(f"[GPS] GNSS coordinates for {device_id}: {coords}")
            except Exception as e:
                logging.warning(f"[GPS] Could not parse GNSS for {device_id}: {e}")
    
    def _process_positioning_data(self, entry: Dict[str, Any], event_type: str, device_id: str, telemetry: Dict[str, Any]):
        """Process trac positioning data with proper caching"""
        trac_data = None
        
        # Handle trac event - new positioning data
        if event_type == "trac":
            data_section = entry.get("data", {})
            if data_section:
                ref = data_section.get("ref")  # Corrected field name
                pos = data_section.get("pos")  # Corrected field name
                timestamp = entry.get("device_ts") or entry.get("server_ts")
                
                if ref:
                    # Use anchor ref to get asset coordinates (pos array not needed)
                    pos = []  # Keep empty for now, using anchor-based positioning
                    self.cache_manager.update_trac_cache(device_id, ref, pos, timestamp)
                    trac_data = TracData(ref=ref, pos=pos, timestamp=timestamp)
                    
                    # Get asset config for this anchor/AP
                    asset_config = self.cache_manager.get_asset_config(ref)
                    if asset_config:
                        logging.info(f"[Trac] Device {device_id} at anchor {ref}, using asset coordinates")
                    else:
                        logging.warning(f"[Trac] No asset config found for anchor {ref}")
                    
                    logging.debug(f"[Trac] New positioning for {device_id}: AP {ref}")
        
        # For other events, use cached trac data
        else:
            trac_data = self.cache_manager.get_trac_cache(device_id)
            if trac_data:
                logging.debug(f"[Trac] Using cached positioning for {device_id}: AP {trac_data.ref}")
        
        # Generate coordinates and check fencing
        
        # Get device type and category FIRST (always run, regardless of trac data)
        device_config = self.cache_manager.get_device_config(device_id)
        device_type = "Unknown"
        category = "Unknown"
        
        if device_config:
            # PRIORITY 1: Use DeviceType server attribute (most reliable)
            if device_config.device_type:
                device_type, category = self.cache_manager._classify_device_type(device_config.device_type)
                logging.info(f"[Analytics] Device {device_id} using DeviceType: '{device_config.device_type}' → {device_type} ({category})")
            
            # PRIORITY 2: Fallback to device name parsing
            elif device_config.name:
                device_type, category = self.cache_manager._classify_device_name(device_config.name)
                logging.info(f"[Analytics] Device {device_id} using Name: '{device_config.name}' → {device_type} ({category})")
            
            # PRIORITY 3: Last resort - device ID patterns
            else:
                device_type, category = self.cache_manager._classify_device_id(device_id)
                logging.info(f"[Analytics] Device {device_id} using ID pattern → {device_type} ({category})")
        else:
            # No device config - use device ID patterns
            device_type, category = self.cache_manager._classify_device_id(device_id)
            logging.warning(f"[Analytics] Device {device_id} has no config, using ID pattern → {device_type} ({category})")
        
        # Add device type to telemetry (always available now)
        telemetry.update({
            "device_type": device_type,
            "category": category
        })
        
        if trac_data:
            # Update location analytics
            self.cache_manager.update_device_location(device_id, trac_data.ref)
            
            # Add trac fields to telemetry
            telemetry.update({
                "trac_pos_ref": trac_data.ref,
                "trac_pos_arr": trac_data.pos,
                "trac_timestamp": parse_kinesis_timestamp_to_ist(trac_data.timestamp)
            })
            
            # Get asset config using anchor ref as asset ID
            asset_config = self.cache_manager.get_asset_config(trac_data.ref)
            if asset_config:
                # Generate coordinates from asset configuration
                coordinates = self.coordinate_generator.generate_coordinates(asset_config)
                telemetry.update(coordinates)
                logging.info(f"[Coords] Device {device_id} at anchor {trac_data.ref}: lat={coordinates['latitude']}, lng={coordinates['longitude']}")
            else:
                logging.warning(f"[Coords] No asset found for anchor {trac_data.ref}, using default coordinates")
                coordinates = self.coordinate_generator.generate_coordinates(None)
                telemetry.update(coordinates)
            
            # Generate multi-level coordinates
            asset_config = self.cache_manager.get_asset_config(trac_data.ref)
            coordinates = self.coordinate_generator.generate_coordinates(asset_config)
            telemetry.update(coordinates)
            
            # Add enhanced device analytics
            visit_analytics = self.cache_manager.get_device_visit_analytics(device_id)
            traffic_rank = self.cache_manager.get_location_traffic_rank(trac_data.ref)
            
            # Get location information
            current_location_name = "Unknown Location"
            location_type = "Unknown"
            department = "Unknown"
            unknown_anchor_id = ""
            
            if asset_config:
                department = asset_config.dept or "Unknown Department"
                location_label = asset_config.label
                
                if location_label and department:
                    # Use meaningful location name with department context
                    current_location_name = f"{location_label} ({department})"
                elif location_label:
                    # Use label only if department is missing
                    current_location_name = location_label
                elif department and department != "Unknown Department":
                    # Fallback to department-based naming
                    current_location_name = f"{department} - Anchor {trac_data.ref}"
                else:
                    # Last resort for assets with no meaningful data
                    current_location_name = f"Unknown Location (Anchor: {trac_data.ref})"
                    unknown_anchor_id = trac_data.ref
                
                # Determine location type from department
                if department and department != "Unknown Department":
                    dept_lower = department.lower()
                    if "corridor" in dept_lower:
                        location_type = "Corridor"
                    elif "icu" in dept_lower:
                        location_type = "ICU"
                    elif "emergency" in dept_lower:
                        location_type = "Emergency Room"
                    elif "ward" in dept_lower or "picu" in dept_lower:
                        location_type = "Ward"
                    else:
                        location_type = "Department"
                else:
                    location_type = "Unknown - Needs Configuration"
            else:
                # For unknown anchors, include the anchor ID for infrastructure team
                unknown_anchor_id = trac_data.ref
                current_location_name = f"Unknown Location (Anchor: {unknown_anchor_id})"
                location_type = "Unknown - Needs Configuration"
                department = "Unknown - Needs Configuration"
            
            # Add enhanced analytics to telemetry
            telemetry.update({
                # Device Analytics
                "device_type": device_type,
                "category": category,
                "current_location_name": current_location_name,
                "location_type": location_type,
                "department": department,
                "location_visit_duration": visit_analytics.get("location_visit_duration", 0),
                "daily_location_visits": visit_analytics.get("daily_location_visits", 0),
                "total_visits": visit_analytics.get("daily_location_visits", 0),
                "location_popularity_rank": traffic_rank,
                "movement_status": "stationary",  # Could be enhanced with movement detection
                "visit_start_time": visit_analytics.get("visit_start_time", ""),
                "unknown_anchor_id": unknown_anchor_id  # For infrastructure team to configure
            })
            
            # Log enhanced analytics
            logging.info(f"[Analytics] 🎯 Device {device_id} ({device_type}): {current_location_name} - Duration: {visit_analytics.get('location_visit_duration', 0)}m, Visits: {visit_analytics.get('daily_location_visits', 0)}, Rank: #{traffic_rank}")
            
            # Check fence violation with corrected logic
            is_violation = self.fence_validator.check_fence_violation(device_config, trac_data.ref)
            telemetry["fence_violation"] = is_violation
            
            if is_violation:
                logging.warning(f"[Fence] ⚠️ Violation detected for {device_id} at AP {trac_data.ref}")
    
    # =========================================================================
    # WEBSOCKET CONNECTION
    # =========================================================================
    
    def _on_open(self, ws):
        """WebSocket connection opened"""
        logging.info("[WS] ✅ Connection established")
        self.reconnect_delay = 5  # Reset backoff on successful connect
        
        # Send subscription request
        sub_request = {
            'hdr': {'cmd': 'sub'},
            'data': {
                'taga': ['tele', 'alert'],
                'tagd': ['status', 'trac']
            }
        }
        
        ws.send(json.dumps(sub_request))
        logging.info("[WS] 📡 Subscription sent")
    
    def _on_message(self, ws, message):
        """Handle incoming WebSocket message"""
        # Daily reset on first event of the day
        today_str = get_ist_now().strftime("%Y-%m-%d")
        if self.cache_manager.last_reset_date != today_str:
            self.cache_manager.check_and_reset_daily_counters()
            self.cache_manager.send_daily_reset_telemetry()
            self.cache_manager.send_daily_reset_asset_telemetry()
        try:
            logging.debug(f"[WS] Raw message: {message}")
            payload = json.loads(message)
            
            if isinstance(payload, list):
                for item in payload:
                    self.process_kinesis_message(item)
            elif isinstance(payload, dict):
                self.process_kinesis_message(payload)
            else:
                logging.warning(f"[WS] Unexpected payload format: {type(payload)}")
                
        except json.JSONDecodeError as e:
            logging.error(f"[WS] ❌ Invalid JSON: {e}")
        except Exception as e:
            logging.error(f"[WS] ❌ Message processing error: {e}")
    
    def _on_error(self, ws, error):
        """Handle WebSocket error"""
        logging.error(f"[WS] ❌ Error: {error}")
        
        # Log additional error details if available
        if hasattr(error, 'args') and error.args:
            logging.error(f"[WS] Error details: {error.args}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close with exponential backoff"""
        logging.info(f"[WS] Connection closed: code={close_status_code}, msg={close_msg}")
        
        # Exponential backoff for reconnection
        self.reconnect_delay = min(self.reconnect_delay * 2, 300)  # Max 5 minutes
        logging.info(f"[WS] Reconnecting in {self.reconnect_delay} seconds...")
        
        time.sleep(self.reconnect_delay)
        
        # Refresh tokens before reconnecting
        try:
            self.authenticate_kinesis()
            self.start_websocket_connection()
        except Exception as e:
            logging.error(f"[WS] ❌ Reconnection failed: {e}")
    
    def start_websocket_connection(self):
        """Start WebSocket connection to Kinesis"""
        try:
            ws_url = (f"wss://wss.alpha.atollkinesis.com/sites/{self.config['SITE_ID']}/"
                     f"{self.config['CLIENT_ID']}/sub?token={self.kinesis_access_token}")
            
            logging.info(f"[WS] Connecting to: wss://wss.alpha.atollkinesis.com/sites/{self.config['SITE_ID']}/...")
            
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            
            ws.run_forever()
            
        except Exception as e:
            logging.error(f"[WS] ❌ Connection error: {e}")
            raise
    
    # =========================================================================
    # MAIN EXECUTION
    # =========================================================================
    
    def initialize(self) -> bool:
        """Initialize all components and caches"""
        try:
            logging.info("[Init] 🚀 Initializing ThingsBoard-Kinesis Bridge...")
            
            # Authenticate with both systems
            if not self.authenticate_thingsboard():
                return False
                
            if not self.authenticate_kinesis():
                return False
            
            # Load complete cache
            logging.info("[Init] 📋 Loading complete cache...")
            
            if not self.cache_manager.load_all_assets():
                logging.error("[Init] ❌ Failed to load assets cache")
                return False
                
            if not self.cache_manager.load_all_devices():
                logging.error("[Init] ❌ Failed to load devices cache")
                return False
            
            # Log cache statistics
            assets_count = len(self.cache_manager.assets)
            devices_count = len(self.cache_manager.devices)
            
            logging.info(f"[Init] ✅ Cache loaded - Assets: {assets_count}, Devices: {devices_count}")
            
            # Export data to CSV for backup
            logging.info("[Init] 📁 Generating CSV exports...")
            self.cache_manager.export_assets_to_csv()
            self.cache_manager.export_devices_to_csv()
            
            return True
            
        except Exception as e:
            logging.error(f"[Init] ❌ Initialization error: {e}")
            return False
    
    def run(self):
        """Main execution method"""
        try:
            if not self.initialize():
                logging.error("[Main] ❌ Initialization failed")
                return
            
            logging.info("[Main] 🚀 Starting WebSocket connection...")
            self.start_websocket_connection()
            
        except KeyboardInterrupt:
            logging.info("[Main] 🛑 Shutdown requested by user")
        except Exception as e:
            logging.error(f"[Main] ❌ Runtime error: {e}")
            raise


def main():
    """Entry point with proper configuration"""
    config = {
        'THINGSBOARD_HOST': os.environ.get('THINGSBOARD_HOST', '172.20.0.2:9090'),
        'THINGSBOARD_USERNAME': os.environ.get('THINGSBOARD_USERNAME', 'sjriiot@sjri.in'),
        'THINGSBOARD_PASSWORD': os.environ.get('THINGSBOARD_PASSWORD', 'sjri@123'),
        'DEVICE_TYPE': os.environ.get('DEVICE_TYPE', 'PoC-Tag'),
        'KINESIS_BASE': os.environ.get('KINESIS_BASE', 'alpha.atollkinesis.com'),
        'SITE_ID': os.environ.get('SITE_ID', 'atoll_stjohn'),
        'CLIENT_ID': os.environ.get('CLIENT_ID', ''),
        'CLIENT_SECRET': os.environ.get('CLIENT_SECRET', ''),
        'LOG_LEVEL': os.environ.get('LOG_LEVEL', 'INFO')  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    }
    
    # Validate required configuration
    required_fields = ['CLIENT_ID', 'CLIENT_SECRET']
    missing_fields = [field for field in required_fields if not config[field]]
    
    if missing_fields:
        # Use basic logging since bridge isn't initialized yet
        print(f"❌ Missing required configuration: {missing_fields}")
        return
    
    # Start the bridge
    bridge = ThingsboardKinesisBridge(config)
    
    # Log configuration info
    logging.info(f"[Main] 🚀 Bridge starting with log level: {bridge.get_current_log_level()}")
    logging.info(f"[Main] 💡 Change log level: export LOG_LEVEL=DEBUG|INFO|WARNING|ERROR")
    logging.info("[Main] 📊 Enhanced Asset Analytics: Utilization tracking, stagnation detection, visit density analytics enabled")
    logging.info("[Main] 🎯 New telemetry keys: usage_state, utilization_rate, daily_usage_hours, days_at_location, visit_density_ranking, asset_demand_level")
    
    bridge.run()


if __name__ == "__main__":
    main()