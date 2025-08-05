#!/usr/bin/env python3
"""
Google Maps Image Downloader for HPCL Sharp Turns and Blind Spots
OPTIMIZED VERSION - Minimizes API costs
Enhanced with multiple API key management and smart download strategies
"""

import os
import requests
import time
import random
import sqlite3
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any 
import logging
from PIL import Image
import io
import threading

logger = logging.getLogger(__name__)

class GoogleMapsImageDownloader:
    """Downloads and manages Google Maps images for sharp turns and blind spots - COST OPTIMIZED"""
    
    def __init__(self, api_key: str = None, base_path: str = "./route_images"):
        """
        Initialize the image downloader with multiple API key support
        
        Args:
            api_key: Google Maps API key (string or list of strings)
            base_path: Base directory for storing images
        """
        # Maintain backward compatibility - accept single key
        if api_key:
            if isinstance(api_key, str):
                self.api_keys = [api_key]
            elif isinstance(api_key, list):
                self.api_keys = [k for k in api_key if k]
            else:
                self.api_keys = []
        else:
            # Try to load from environment
            self.api_keys = self._load_keys_from_env()
        
        if not self.api_keys:
            logger.warning("No API keys provided!")
        
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # API endpoints
        self.street_view_url = "https://maps.googleapis.com/maps/api/streetview"
        self.street_view_metadata_url = "https://maps.googleapis.com/maps/api/streetview/metadata"
        self.static_map_url = "https://maps.googleapis.com/maps/api/staticmap"
        
        # Rate limiting
        self.request_delay = 0.1  # 100ms between requests
        self.last_request_time = 0
        
        # Cost optimization settings
        self.enable_metadata_check = True  # Check if street view exists before downloading
        self.max_markers_per_map = 100     # Maximum markers on one static map
        self.prioritize_high_risk = True   # Only download for high-risk points
        self.risk_threshold = 7            # Minimum risk score for individual downloads
        
        # Database for tracking API usage
        self.db_path = self.base_path / "api_usage.db"
        self.db_lock = threading.Lock()
        self._init_database()
        
        # For backward compatibility - use first key as default
        if self.api_keys:
            self.api_key = self.api_keys[0]
        else:
            self.api_key = None
        
        logger.info(f"✅ Cost-Optimized Image Downloader initialized with {len(self.api_keys)} API keys")
    
    def _load_keys_from_env(self) -> List[str]:
        """Load API keys from environment variables"""
        keys = []
        
        # First check the original environment variable for backward compatibility
        original_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if original_key:
            keys.append(original_key)
        
        # Try multiple environment variable patterns
        patterns = [
            "GOOGLE_MAPS_API_KEY_1",
            "GOOGLE_MAPS_API_KEY_2",
            "GOOGLE_MAPS_API_KEY_3",
            "GOOGLE_API_KEY_1",
            "GOOGLE_API_KEY_2",
            "GOOGLE_API_KEY_3"
        ]
        
        for pattern in patterns:
            key = os.getenv(pattern)
            if key and key not in keys:
                keys.append(key)
        
        # Also check for comma-separated keys
        multi_keys = os.getenv("GOOGLE_MAPS_API_KEYS", "")
        if multi_keys:
            for key in multi_keys.split(","):
                key = key.strip()
                if key and key not in keys:
                    keys.append(key)
        
        return keys
    
    def _init_database(self):
        """Initialize SQLite database for API usage tracking"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_key TEXT UNIQUE NOT NULL,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1,
                    monthly_limit INTEGER DEFAULT 50000,
                    notes TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usage_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_key TEXT NOT NULL,
                    request_type TEXT NOT NULL,
                    request_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN DEFAULT 1,
                    response_code INTEGER,
                    image_type TEXT,
                    route_id TEXT,
                    turn_id TEXT,
                    latitude REAL,
                    longitude REAL,
                    file_path TEXT,
                    error_message TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_usage (
                    usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    api_key TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    total_requests INTEGER DEFAULT 0,
                    successful_requests INTEGER DEFAULT 0,
                    failed_requests INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(api_key, year, month)
                )
            """)
            
            # Add indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_usage_date ON usage_log(request_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_usage_key ON usage_log(api_key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_monthly_key ON monthly_usage(api_key)")
            
            # Register API keys
            for key in self.api_keys:
                cursor.execute("""
                    INSERT OR IGNORE INTO api_keys (api_key) VALUES (?)
                """, (key,))
            
            conn.commit()
            conn.close()
    
    def _get_available_key(self) -> Optional[str]:
        """Get an available API key that hasn't exceeded quota"""
        if not self.api_keys:
            return None
        
        # If only one key, return it (backward compatibility)
        if len(self.api_keys) == 1:
            return self.api_keys[0]
        
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            # Shuffle keys for random selection
            available_keys = self.api_keys.copy()
            random.shuffle(available_keys)
            
            for key in available_keys:
                # Check if key is active
                cursor.execute("""
                    SELECT is_active, monthly_limit FROM api_keys WHERE api_key = ?
                """, (key,))
                result = cursor.fetchone()
                
                if result and not result[0]:  # Key is deactivated
                    continue
                
                monthly_limit = result[1] if result else 50000
                
                # Check current month's usage
                cursor.execute("""
                    SELECT total_requests FROM monthly_usage 
                    WHERE api_key = ? AND year = ? AND month = ?
                """, (key, current_year, current_month))
                
                usage_result = cursor.fetchone()
                current_usage = usage_result[0] if usage_result else 0
                
                if current_usage < monthly_limit:
                    conn.close()
                    logger.info(f"🔑 Selected API key with {monthly_limit - current_usage} requests remaining this month")
                    return key
            
            conn.close()
            logger.error("❌ All API keys have exceeded their monthly quotas!")
            return None
    
    def _log_api_usage(self, api_key: str, request_type: str, success: bool, 
                      response_code: int = None, image_type: str = None,
                      route_id: str = None, turn_id: str = None,
                      latitude: float = None, longitude: float = None,
                      file_path: str = None, error_message: str = None):
        """Log API usage to database"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Log the request
            cursor.execute("""
                INSERT INTO usage_log (
                    api_key, request_type, success, response_code, image_type,
                    route_id, turn_id, latitude, longitude, file_path, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (api_key, request_type, success, response_code, image_type,
                  route_id, turn_id, latitude, longitude, file_path, error_message))
            
            # Update monthly usage
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            cursor.execute("""
                INSERT INTO monthly_usage (api_key, year, month, total_requests, 
                                         successful_requests, failed_requests)
                VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT(api_key, year, month) DO UPDATE SET
                    total_requests = total_requests + 1,
                    successful_requests = successful_requests + ?,
                    failed_requests = failed_requests + ?,
                    last_updated = CURRENT_TIMESTAMP
            """, (api_key, current_year, current_month, 
                  1 if success else 0, 0 if success else 1,
                  1 if success else 0, 0 if success else 1))
            
            conn.commit()
            conn.close()
    
    def _rate_limit(self):
        """Implement rate limiting to avoid API quota issues"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()
    
    def check_street_view_availability(self, lat: float, lng: float, api_key: str) -> bool:
        """
        Check if street view is available at location (uses metadata API - cheaper)
        Metadata requests are FREE or much cheaper than actual image requests
        """
        if not self.enable_metadata_check:
            return True  # Skip check if disabled
        
        try:
            self._rate_limit()
            
            params = {
                'location': f'{lat},{lng}',
                'key': api_key
            }
            
            response = requests.get(self.street_view_metadata_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                is_available = data.get('status') == 'OK'
                
                # Log metadata check (these are usually free or very cheap)
                self._log_api_usage(
                    api_key=api_key,
                    request_type='streetview_metadata',
                    success=True,
                    response_code=200,
                    latitude=lat,
                    longitude=lng,
                    error_message=None if is_available else 'No street view coverage'
                )
                
                if not is_available:
                    logger.warning(f"❌ No street view available at {lat},{lng}")
                
                return is_available
            else:
                return False
                
        except Exception as e:
            logger.error(f"Error checking street view availability: {e}")
            return False
    
    def download_route_overview_map(self, route_data: Dict, route_id: str, 
                                   include_all_risks: bool = True) -> Optional[str]:
        """
        Download ONE comprehensive map showing ALL risk points (MOST COST EFFECTIVE)
        This replaces dozens of individual API calls with just ONE
        """
        try:
            # Generate filename
            route_folder = self.get_route_image_folder(route_id)
            filename = "route_comprehensive_risk_overview.png"
            filepath = route_folder / filename
            
            # Check if already exists
            if filepath.exists():
                logger.info(f"✅ Comprehensive overview already exists: {filepath}")
                return str(filepath)
            
            # Get available API key
            api_key = self._get_available_key()
            if not api_key:
                logger.error("❌ No available API keys with remaining quota")
                return None
            
            # Prepare map parameters
            params = {
                'size': '800x600',
                'maptype': 'roadmap',
                'key': api_key,
                'scale': 2,  # Higher quality
                'format': 'png'
            }
            
            # Collect all risk points
            all_markers = []
            
            # Get route data
            route = route_data.get('route', {})
            collections = route_data.get('collections', {})
            
            # Add start and end points
            start_coords = route.get('fromCoordinates', {})
            end_coords = route.get('toCoordinates', {})
            
            if start_coords:
                all_markers.append(f"color:green|size:mid|label:S|{start_coords.get('latitude')},{start_coords.get('longitude')}")
            if end_coords:
                all_markers.append(f"color:red|size:mid|label:E|{end_coords.get('latitude')},{end_coords.get('longitude')}")
            
            if include_all_risks:
                # Add sharp turns (only high risk)
                sharp_turns = collections.get('sharp_turns', [])
                high_risk_turns = [t for t in sharp_turns if t.get('riskScore', 0) >= self.risk_threshold]
                for i, turn in enumerate(high_risk_turns[:25]):  # Limit to 25
                    lat, lng = turn.get('latitude'), turn.get('longitude')
                    if lat and lng:
                        all_markers.append(f"color:orange|size:small|label:T|{lat},{lng}")
                
                # Add blind spots (only high risk)
                blind_spots = collections.get('blind_spots', [])
                high_risk_spots = [s for s in blind_spots if s.get('riskScore', 0) >= self.risk_threshold]
                for i, spot in enumerate(high_risk_spots[:25]):  # Limit to 25
                    lat, lng = spot.get('latitude'), spot.get('longitude')
                    if lat and lng:
                        all_markers.append(f"color:red|size:small|label:B|{lat},{lng}")
                
                # Add critical emergency services
                emergency_services = collections.get('emergency_services', [])
                hospitals = [e for e in emergency_services if e.get('serviceType') == 'hospital']
                for i, hospital in enumerate(hospitals[:10]):  # Limit to 10
                    lat, lng = hospital.get('latitude'), hospital.get('longitude')
                    if lat and lng:
                        all_markers.append(f"color:blue|size:tiny|label:H|{lat},{lng}")
            
            # Add route path if available
            route_points = route.get('routePoints', [])
            if route_points and len(route_points) > 1:
                # Simplify route to avoid URL length issues
                step = max(1, len(route_points) // 50)  # Max 50 points
                path_points = [f"{p['latitude']},{p['longitude']}" for p in route_points[::step]]
                if path_points:
                    params['path'] = f"color:0x0000ff|weight:3|{('|').join(path_points)}"
            
            # Add all markers
            if all_markers:
                # Limit total markers to avoid URL length issues
                all_markers = all_markers[:self.max_markers_per_map]
                params['markers'] = '|'.join(all_markers)
            
            # Make the API call
            self._rate_limit()
            logger.info(f"📍 Downloading comprehensive map with {len(all_markers)} risk points")
            
            response = requests.get(self.static_map_url, params=params, timeout=30)
            
            if response.status_code == 200:
                # Save image
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Comprehensive overview saved: {filepath}")
                logger.info(f"💰 Saved ~{len(all_markers)} individual API calls!")
                
                # Log successful usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap_comprehensive',
                    success=True,
                    response_code=200,
                    image_type='comprehensive_overview',
                    route_id=route_id,
                    file_path=str(filepath)
                )
                
                return str(filepath)
            else:
                logger.error(f"Failed to download comprehensive map: {response.status_code}")
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap_comprehensive',
                    success=False,
                    response_code=response.status_code,
                    error_message=f"HTTP {response.status_code}"
                )
                return None
                
        except Exception as e:
            logger.error(f"Error downloading comprehensive map: {e}")
            return None
    
    def get_route_image_folder(self, route_id: str) -> Path:
        """Get or create folder for route images"""
        route_folder = self.base_path / f"route_{route_id}"
        route_folder.mkdir(parents=True, exist_ok=True)
        return route_folder
    
    def download_street_view_image(self, lat: float, lng: float, 
                             route_id: str, turn_id: str, 
                             heading: Optional[float] = None,
                             fov: int = 90, pitch: int = 0,
                             force_download: bool = False) -> Optional[str]:
        """
        Download street view image for a specific location
        OPTIMIZED: Checks availability first to avoid wasted API calls
        """
        try:
            # Calculate heading if not provided
            if heading is None:
                heading = 0
            
            # Generate filename
            route_folder = self.get_route_image_folder(route_id)
            filename = f"streetview_{turn_id}_h{int(heading)}.jpg"
            filepath = route_folder / filename
            
            # Check if image already exists (CACHING LOGIC PRESERVED)
            if filepath.exists() and not force_download:
                logger.info(f"✅ Street view already exists: {filepath}")
                return str(filepath)
            
            # Get available API key
            api_key = self._get_available_key()
            if not api_key:
                logger.error("❌ No available API keys with remaining quota")
                return None
            
            # COST OPTIMIZATION: Check if street view exists before downloading
            if not self.check_street_view_availability(lat, lng, api_key):
                return None  # No street view available, don't waste API call
            
            # Rate limit before making API call
            self._rate_limit()
            
            params = {
                'location': f'{lat},{lng}',
                'size': '640x480',
                'fov': fov,
                'pitch': pitch,
                'heading': heading,
                'key': api_key
            }
            
            logger.info(f"Downloading street view for turn {turn_id} at {lat},{lng}")
            response = requests.get(self.street_view_url, params=params, timeout=30)
            
            if response.status_code == 200:
                # Save image
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Street view saved: {filepath}")
                
                # Log successful usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='streetview',
                    success=True,
                    response_code=200,
                    image_type='street_view',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    file_path=str(filepath)
                )
                
                return str(filepath)
            else:
                logger.error(f"Failed to download street view: {response.status_code}")
                
                # Log failed usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='streetview',
                    success=False,
                    response_code=response.status_code,
                    image_type='street_view',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    error_message=f"HTTP {response.status_code}"
                )
                
                return None
                
        except Exception as e:
            logger.error(f"Error downloading street view: {e}")
            if 'api_key' in locals():
                self._log_api_usage(
                    api_key=api_key,
                    request_type='streetview',
                    success=False,
                    image_type='street_view',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    error_message=str(e)
                )
            return None
    
    def download_satellite_image(self, lat: float, lng: float, 
                           route_id: str, turn_id: str,
                           zoom: int = 18,
                           force_download: bool = False) -> Optional[str]:
        """
        Download satellite image for a specific location
        """
        try:
            # Generate filename
            route_folder = self.get_route_image_folder(route_id)
            filename = f"satellite_{turn_id}.png"
            filepath = route_folder / filename
            
            # Check if image already exists (CACHING LOGIC PRESERVED)
            if filepath.exists() and not force_download:
                logger.info(f"✅ Satellite view already exists: {filepath}")
                return str(filepath)
            
            # Get available API key
            api_key = self._get_available_key()
            if not api_key:
                logger.error("❌ No available API keys with remaining quota")
                return None
            
            # Rate limit before making API call
            self._rate_limit()
            
            params = {
                'center': f'{lat},{lng}',
                'zoom': zoom,
                'size': '640x480',
                'maptype': 'satellite',
                'key': api_key,
                'scale': 2  # Higher quality
            }
            
            logger.info(f"Downloading satellite view for turn {turn_id} at {lat},{lng}")
            response = requests.get(self.static_map_url, params=params, timeout=30)
            
            if response.status_code == 200:
                # Save image
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Satellite view saved: {filepath}")
                
                # Log successful usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap',
                    success=True,
                    response_code=200,
                    image_type='satellite',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    file_path=str(filepath)
                )
                
                return str(filepath)
            else:
                logger.error(f"Failed to download satellite view: {response.status_code}")
                
                # Log failed usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap',
                    success=False,
                    response_code=response.status_code,
                    image_type='satellite',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    error_message=f"HTTP {response.status_code}"
                )
                
                return None
                
        except Exception as e:
            logger.error(f"Error downloading satellite view: {e}")
            if 'api_key' in locals():
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap',
                    success=False,
                    image_type='satellite',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    error_message=str(e)
                )
            return None

    def download_turn_images(self, turn_data: Dict, route_id: str, 
                            force_download: bool = False) -> Dict[str, str]:
        """
        OPTIMIZED: Download images for a sharp turn only if high risk
        """
        turn_id = str(turn_data.get('_id', ''))
        risk_score = turn_data.get('riskScore', 0)
        
        # COST OPTIMIZATION: Skip low-risk turns
        if self.prioritize_high_risk and risk_score < self.risk_threshold:
            logger.info(f"⏭️ Skipping low-risk turn {turn_id} (risk: {risk_score})")
            return {}
        
        # Check existing images
        if not force_download:
            status = self.get_image_status(route_id, turn_id)
            logger.info(f"Image status for turn {turn_id}: {sum(status.values())}/6 images exist")
        
        lat = turn_data.get('latitude')
        lng = turn_data.get('longitude')
        turn_angle = turn_data.get('turnAngle', 0)
        
        # Determine risk level
        if risk_score >= 8:
            risk_level = 'critical'
        elif risk_score >= 6:
            risk_level = 'high'
        elif risk_score >= 4:
            risk_level = 'medium'
        else:
            risk_level = 'low'
        
        # Calculate heading based on turn direction
        heading = 0  # Default north
        if turn_angle > 90:
            heading = 90  # Look east for sharp right turns
        elif turn_angle < -90:
            heading = 270  # Look west for sharp left turns
        
        images = {}
        
        # COST OPTIMIZATION: Only download ONE street view for turns
        street_view_path = self.download_street_view_image(
            lat, lng, route_id, turn_id, heading=heading, force_download=force_download
        )
        if street_view_path:
            images['street_view'] = street_view_path
        
        # COST OPTIMIZATION: Skip satellite for medium risk
        if risk_score >= 7:
            satellite_path = self.download_satellite_image(
                lat, lng, route_id, turn_id, zoom=18, force_download=force_download
            )
            if satellite_path:
                images['satellite'] = satellite_path
        
        # COST OPTIMIZATION: Skip roadmap - use comprehensive overview instead
        
        return images

    def download_blind_spot_images(self, spot_data: Dict, route_id: str,
                                force_download: bool = False) -> Dict[str, str]:
        """
        OPTIMIZED: Download images for blind spots based on risk and type
        """
        lat = spot_data.get('latitude')
        lng = spot_data.get('longitude')
        spot_id = str(spot_data.get('_id', ''))
        risk_score = spot_data.get('riskScore', 0)
        spot_type = spot_data.get('spotType', 'unknown')
        
        # COST OPTIMIZATION: Skip low-risk blind spots
        if self.prioritize_high_risk and risk_score < self.risk_threshold:
            logger.info(f"⏭️ Skipping low-risk blind spot {spot_id} (risk: {risk_score})")
            return {}
        
        # Check existing images
        if not force_download:
            status = self.get_image_status(route_id, f"blind_{spot_id}")
            logger.info(f"Image status for blind spot {spot_id}: {sum(status.values())}/5 images exist")
        
        images = {}
        
        # COST OPTIMIZATION: Reduce angles based on spot type
        if spot_type == 'intersection' and risk_score >= 8:
            # For critical intersections, get all 4 directions
            headings = [0, 90, 180, 270]
        elif spot_type == 'curve' and risk_score >= 7:
            # For curves, only get approach directions
            headings = [0, 180]
        else:
            # For other types, only get main direction
            headings = [0]
        
        # Download street views for selected headings
        for heading in headings:
            street_view_path = self.download_street_view_image(
                lat, lng, route_id, f"blind_{spot_id}", heading=heading, force_download=force_download
            )
            if street_view_path:
                images[f'street_view_{heading}'] = street_view_path
        
        # COST OPTIMIZATION: Satellite only for critical blind spots
        if risk_score >= 8:
            satellite_path = self.download_satellite_image(
                lat, lng, route_id, f"blind_{spot_id}", zoom=19, force_download=force_download
            )
            if satellite_path:
                images['satellite'] = satellite_path
        
        return images
    
    def download_route_images_smart(self, route_data: Dict, route_id: str,
                                   max_individual_downloads: int = 10) -> Dict[str, Any]:
        """
        SMART DOWNLOAD STRATEGY - Minimizes costs while maximizing coverage
        
        Strategy:
        1. Download ONE comprehensive overview map (1 API call)
        2. Download street views only for TOP critical points
        3. Use metadata checks to avoid wasted calls
        
        Returns:
            Dictionary with all downloaded image paths and statistics
        """
        logger.info("🚀 Starting smart image download strategy")
        
        downloaded_images = {
            'overview': None,
            'sharp_turns': {},
            'blind_spots': {},
            'statistics': {
                'total_api_calls': 0,
                'saved_api_calls': 0,
                'total_risk_points': 0,
                'downloaded_points': 0
            }
        }
        
        collections = route_data.get('collections', {})
        
        # Step 1: Download comprehensive overview (1 API call)
        logger.info("📍 Step 1: Downloading comprehensive overview map")
        overview_path = self.download_route_overview_map(route_data, route_id)
        if overview_path:
            downloaded_images['overview'] = overview_path
            downloaded_images['statistics']['total_api_calls'] += 1
        
        # Step 2: Identify critical points
        all_sharp_turns = collections.get('sharp_turns', [])
        all_blind_spots = collections.get('blind_spots', [])
        
        # Sort by risk score
        critical_turns = sorted(
            [t for t in all_sharp_turns if t.get('riskScore', 0) >= self.risk_threshold],
            key=lambda x: x.get('riskScore', 0),
            reverse=True
        )
        
        critical_spots = sorted(
            [s for s in all_blind_spots if s.get('riskScore', 0) >= self.risk_threshold],
            key=lambda x: x.get('riskScore', 0),
            reverse=True
        )
        
        total_critical = len(critical_turns) + len(critical_spots)
        downloaded_images['statistics']['total_risk_points'] = total_critical
        
        logger.info(f"📊 Found {len(critical_turns)} critical turns and {len(critical_spots)} critical blind spots")
        
        # Step 3: Download individual images for TOP critical points only
        remaining_budget = max_individual_downloads
        
        # Prioritize by combining all critical points and sorting by risk
        all_critical_points = []
        
        for turn in critical_turns:
            all_critical_points.append({
                'type': 'turn',
                'data': turn,
                'risk': turn.get('riskScore', 0)
            })
        
        for spot in critical_spots:
            all_critical_points.append({
                'type': 'spot',
                'data': spot,
                'risk': spot.get('riskScore', 0)
            })
        
        # Sort all by risk score
        all_critical_points.sort(key=lambda x: x['risk'], reverse=True)
        
        # Download images for top critical points
        for point in all_critical_points[:remaining_budget]:
            if point['type'] == 'turn':
                turn_images = self.download_turn_images(point['data'], route_id)
                if turn_images:
                    turn_id = str(point['data'].get('_id', ''))
                    downloaded_images['sharp_turns'][turn_id] = turn_images
                    downloaded_images['statistics']['total_api_calls'] += len(turn_images)
                    downloaded_images['statistics']['downloaded_points'] += 1
            else:
                spot_images = self.download_blind_spot_images(point['data'], route_id)
                if spot_images:
                    spot_id = str(point['data'].get('_id', ''))
                    downloaded_images['blind_spots'][spot_id] = spot_images
                    downloaded_images['statistics']['total_api_calls'] += len(spot_images)
                    downloaded_images['statistics']['downloaded_points'] += 1
        
        # Calculate saved API calls
        potential_calls = total_critical * 3  # Each point could need 3 images
        actual_calls = downloaded_images['statistics']['total_api_calls']
        downloaded_images['statistics']['saved_api_calls'] = potential_calls - actual_calls
        
        # Log summary
        stats = downloaded_images['statistics']
        logger.info("="*50)
        logger.info("💰 COST OPTIMIZATION SUMMARY:")
        logger.info(f"📍 Total risk points found: {stats['total_risk_points']}")
        logger.info(f"📷 Points with individual images: {stats['downloaded_points']}")
        logger.info(f"🔗 Total API calls made: {stats['total_api_calls']}")
        logger.info(f"💵 API calls saved: {stats['saved_api_calls']}")
        logger.info(f"📊 Coverage: {(stats['downloaded_points']/stats['total_risk_points']*100):.1f}% of critical points")
        logger.info("="*50)
        
        return downloaded_images
    
    def get_image_status(self, route_id: str, turn_id: str) -> Dict[str, bool]:
        """
        Check which images already exist for a turn/spot
        
        Returns dict with status of each image type
        """
        route_folder = self.get_route_image_folder(route_id)
        
        status = {
            'street_view': (route_folder / f"streetview_{turn_id}_h0.jpg").exists(),
            'street_view_90': (route_folder / f"streetview_{turn_id}_h90.jpg").exists(),
            'street_view_180': (route_folder / f"streetview_{turn_id}_h180.jpg").exists(),
            'street_view_270': (route_folder / f"streetview_{turn_id}_h270.jpg").exists(),
            'satellite': (route_folder / f"satellite_{turn_id}.png").exists(),
            'roadmap': (route_folder / f"roadmap_{turn_id}.png").exists(),
            'overview': (route_folder / "route_comprehensive_risk_overview.png").exists()
        }
        
        status['all_exist'] = all([
            status['street_view'] or status['street_view_90'],  # At least one street view
            status['satellite'],
            status['overview']  # Changed from roadmap to overview
        ])
        
        return status
    
    def get_usage_stats(self, api_key: str = None) -> Dict[str, Any]:
        """Get usage statistics for API keys"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            if api_key:
                # Stats for specific key
                cursor.execute("""
                    SELECT 
                        mu.total_requests,
                        mu.successful_requests,
                        mu.failed_requests,
                        ak.monthly_limit
                    FROM monthly_usage mu
                    JOIN api_keys ak ON mu.api_key = ak.api_key
                    WHERE mu.api_key = ? AND mu.year = ? AND mu.month = ?
                """, (api_key, current_year, current_month))
                
                result = cursor.fetchone()
                if result:
                    return {
                        'api_key': api_key[:10] + '...',
                        'total_requests': result[0],
                        'successful_requests': result[1],
                        'failed_requests': result[2],
                        'monthly_limit': result[3],
                        'remaining': result[3] - result[0]
                    }
            else:
                # Stats for all keys
                cursor.execute("""
                    SELECT 
                        mu.api_key,
                        mu.total_requests,
                        mu.successful_requests,
                        mu.failed_requests,
                        ak.monthly_limit,
                        ak.is_active
                    FROM monthly_usage mu
                    JOIN api_keys ak ON mu.api_key = ak.api_key
                    WHERE mu.year = ? AND mu.month = ?
                """, (current_year, current_month))
                
                results = cursor.fetchall()
                stats = []
                for row in results:
                    stats.append({
                        'api_key': row[0][:10] + '...',
                        'total_requests': row[1],
                        'successful_requests': row[2],
                        'failed_requests': row[3],
                        'monthly_limit': row[4],
                        'remaining': row[4] - row[1],
                        'is_active': row[5]
                    })
                
                conn.close()
                return {'keys': stats, 'total_keys': len(self.api_keys)}
            
            conn.close()
            return {}
    
    def create_image_composite(self, images: List[str], output_path: str,
                             layout: str = 'horizontal') -> Optional[str]:
        """
        Create a composite image from multiple images
        """
        try:
            if not images:
                return None
            
            # Load images
            pil_images = []
            for img_path in images:
                if os.path.exists(img_path):
                    img = Image.open(img_path)
                    pil_images.append(img)
            
            if not pil_images:
                return None
            
            # Calculate composite size
            if layout == 'horizontal':
                width = sum(img.width for img in pil_images)
                height = max(img.height for img in pil_images)
            else:
                width = max(img.width for img in pil_images)
                height = sum(img.height for img in pil_images)
            
            # Create composite
            composite = Image.new('RGB', (width, height))
            
            # Paste images
            x_offset = 0
            y_offset = 0
            for img in pil_images:
                if layout == 'horizontal':
                    composite.paste(img, (x_offset, 0))
                    x_offset += img.width
                else:
                    composite.paste(img, (0, y_offset))
                    y_offset += img.height
            
            # Save composite
            composite.save(output_path)
            logger.info(f"✅ Composite image created: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating composite image: {e}")
            return None
    
    def cleanup_old_images(self, route_id: str, days_old: int = 30):
        """Clean up old images for a route"""
        route_folder = self.get_route_image_folder(route_id)
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        
        for file_path in route_folder.glob("*"):
            if file_path.is_file():
                if os.path.getmtime(file_path) < cutoff_time:
                    file_path.unlink()
                    logger.info(f"Deleted old image: {file_path}")
    
    def clear_route_cache(self, route_id: str):
        """Clear all cached images for a specific route"""
        route_folder = self.get_route_image_folder(route_id)
        if route_folder.exists():
            import shutil
            shutil.rmtree(route_folder)
            logger.info(f"Cleared image cache for route {route_id}")
            route_folder.mkdir(parents=True, exist_ok=True)

    def get_cache_stats(self, route_id: str) -> Dict[str, Any]:
        """Get statistics about cached images for a route"""
        route_folder = self.get_route_image_folder(route_id)
        
        if not route_folder.exists():
            return {
                'exists': False,
                'total_images': 0,
                'total_size_mb': 0,
                'image_types': {}
            }
        
        total_size = 0
        image_types = {
            'streetview': 0,
            'satellite': 0,
            'roadmap': 0,
            'overview': 0
        }
        
        for file_path in route_folder.glob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size
                
                if 'streetview' in file_path.name:
                    image_types['streetview'] += 1
                elif 'satellite' in file_path.name:
                    image_types['satellite'] += 1
                elif 'roadmap' in file_path.name:
                    image_types['roadmap'] += 1
                elif 'overview' in file_path.name:
                    image_types['overview'] += 1
        
        return {
            'exists': True,
            'total_images': sum(image_types.values()),
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'image_types': image_types,
            'folder_path': str(route_folder)
        }
    
    # Additional methods for API key management
    
    def add_api_key(self, api_key: str, monthly_limit: int = 50000, notes: str = None):
        """Add a new API key to the system"""
        if api_key not in self.api_keys:
            self.api_keys.append(api_key)
        
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    INSERT INTO api_keys (api_key, monthly_limit, notes)
                    VALUES (?, ?, ?)
                """, (api_key, monthly_limit, notes))
                
                conn.commit()
                logger.info(f"✅ Added new API key with {monthly_limit} monthly limit")
                
            except sqlite3.IntegrityError:
                logger.warning(f"⚠️ API key already exists in database")
            
            conn.close()
    
    def get_usage_report(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Generate usage report for all API keys"""
        with self.db_lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Default to current month if no dates provided
            if not start_date:
                start_date = datetime.now().replace(day=1).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            # Get usage summary
            cursor.execute("""
                SELECT 
                    api_key,
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
                    COUNT(DISTINCT route_id) as unique_routes,
                    COUNT(DISTINCT turn_id) as unique_turns
                FROM usage_log
                WHERE DATE(request_date) BETWEEN ? AND ?
                GROUP BY api_key
            """, (start_date, end_date))
            
            results = cursor.fetchall()
            
            report = {
                'period': f"{start_date} to {end_date}",
                'api_keys': []
            }
            
            total_requests = 0
            total_successful = 0
            total_failed = 0
            
            for row in results:
                key_report = {
                    'api_key': row[0][:10] + '...',
                    'total_requests': row[1],
                    'successful': row[2],
                    'failed': row[3],
                    'success_rate': round(row[2] / row[1] * 100, 2) if row[1] > 0 else 0,
                    'unique_routes': row[4],
                    'unique_turns': row[5]
                }
                
                # Get monthly limit
                cursor.execute("""
                    SELECT monthly_limit FROM api_keys WHERE api_key = ?
                """, (row[0],))
                limit_result = cursor.fetchone()
                key_report['monthly_limit'] = limit_result[0] if limit_result else 50000
                key_report['usage_percentage'] = round(row[1] / key_report['monthly_limit'] * 100, 2)
                
                report['api_keys'].append(key_report)
                
                total_requests += row[1]
                total_successful += row[2]
                total_failed += row[3]
            
            report['summary'] = {
                'total_requests': total_requests,
                'total_successful': total_successful,
                'total_failed': total_failed,
                'overall_success_rate': round(total_successful / total_requests * 100, 2) if total_requests > 0 else 0
            }
            
            conn.close()
            return report


# # ============================================================================
# # EXAMPLE USAGE - COST OPTIMIZED
# # ============================================================================
# if __name__ == "__main__":
#     # Example: Multiple API keys can be provided
#     api_keys = [
#         os.getenv('GOOGLE_MAPS_API_KEY_1'),
#         os.getenv('GOOGLE_MAPS_API_KEY_2'),
#         os.getenv('GOOGLE_MAPS_API_KEY_3'),
#     ]
    
#     # Filter out None values
#     api_keys = [k for k in api_keys if k]
    
#     # Or load from comma-separated environment variable
#     if not api_keys:
#         multi_keys = os.getenv('GOOGLE_MAPS_API_KEYS', '')
#         if multi_keys:
#             api_keys = [k.strip() for k in multi_keys.split(',') if k.strip()]
    
#     # For backward compatibility - also works with single key
#     if not api_keys:
#         single_key = os.getenv('GOOGLE_MAPS_API_KEY')
#         if single_key:
#             api_keys = [single_key]
    
#     downloader = GoogleMapsImageDownloader(api_key=api_keys if len(api_keys) > 1 else (api_keys[0] if api_keys else None))
    
#     # Configure cost optimization
#     downloader.enable_metadata_check = True      # Check availability first
#     downloader.prioritize_high_risk = True       # Only download for high-risk points
#     downloader.risk_threshold = 7                # Minimum risk score
#     downloader.max_markers_per_map = 100        # Max markers on overview
    
#     # Example route data with multiple risk points
#     example_route_data = {
#         'route': {
#             '_id': 'test_route_123',
#             'fromCoordinates': {'latitude': 19.0760, 'longitude': 72.8777},
#             'toCoordinates': {'latitude': 19.1176, 'longitude': 72.9060},
#             'routePoints': [
#                 {'latitude': 19.0760, 'longitude': 72.8777},
#                 {'latitude': 19.0900, 'longitude': 72.8900},
#                 {'latitude': 19.1176, 'longitude': 72.9060}
#             ]
#         },
#         'collections': {
#             'sharp_turns': [
#                 {'_id': '1', 'latitude': 19.0850, 'longitude': 72.8850, 'riskScore': 9, 'turnAngle': 95},
#                 {'_id': '2', 'latitude': 19.0950, 'longitude': 72.8950, 'riskScore': 7, 'turnAngle': 80},
#                 {'_id': '3', 'latitude': 19.1050, 'longitude': 72.9000, 'riskScore': 5, 'turnAngle': 60},  # Will be skipped
#             ],
#             'blind_spots': [
#                 {'_id': '1', 'latitude': 19.0800, 'longitude': 72.8800, 'riskScore': 8, 'spotType': 'intersection'},
#                 {'_id': '2', 'latitude': 19.1000, 'longitude': 72.8980, 'riskScore': 6, 'spotType': 'curve'},  # Will be skipped
#             ],
#             'emergency_services': [
#                 {'serviceType': 'hospital', 'latitude': 19.0900, 'longitude': 72.8850}
#             ]
#         }
#     }
    
#     # Use smart download strategy
#     print("\n💰 COST-OPTIMIZED IMAGE DOWNLOAD STRATEGY")
#     print("="*50)
    
#     result = downloader.download_route_images_smart(
#         example_route_data, 
#         'test_route_123',
#         max_individual_downloads=5  # Limit individual downloads
#     )
    
#     print(f"\n✅ Download complete!")
#     print(f"📊 Statistics: {json.dumps(result['statistics'], indent=2)}")
    
#     # Get usage statistics
#     stats = downloader.get_usage_stats()
#     print(f"\n📈 API Usage Stats: {json.dumps(stats, indent=2)}")
    
#     # Get cache statistics
#     cache_stats = downloader.get_cache_stats('test_route_123')
#     print(f"\n💾 Cache Stats: {json.dumps(cache_stats, indent=2)}")