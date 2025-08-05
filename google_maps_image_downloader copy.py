#!/usr/bin/env python3
"""
Google Maps Image Downloader for HPCL Sharp Turns and Blind Spots
Downloads Street View and Satellite images for each critical point
Enhanced with multiple API key management and quota tracking
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
    """Downloads and manages Google Maps images for sharp turns and blind spots"""
    
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
        self.static_map_url = "https://maps.googleapis.com/maps/api/staticmap"
        
        # Rate limiting
        self.request_delay = 0.1  # 100ms between requests
        self.last_request_time = 0
        
        # Database for tracking API usage
        self.db_path = self.base_path / "api_usage.db"
        self.db_lock = threading.Lock()
        self._init_database()
        
        # For backward compatibility - use first key as default
        if self.api_keys:
            self.api_key = self.api_keys[0]
        else:
            self.api_key = None
        
        logger.info(f"✅ Image Downloader initialized with {len(self.api_keys)} API keys")
    
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
    
    def _rate_limit(self):
        """Implement rate limiting to avoid API quota issues"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()
    
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
        
        Args:
            lat: Latitude
            lng: Longitude
            route_id: Route identifier
            turn_id: Turn/spot identifier
            heading: Camera heading (0-360, 0=North, 90=East)
            fov: Field of view (10-120)
            pitch: Up/down angle (-90 to 90)
            force_download: Force re-download even if file exists
        
        Returns:
            Path to saved image or None if failed
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
        
        Args:
            lat: Latitude
            lng: Longitude
            route_id: Route identifier
            turn_id: Turn/spot identifier
            zoom: Zoom level (1-20, higher = more detail)
            force_download: Force re-download even if file exists
        
        Returns:
            Path to saved image or None if failed
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

    def download_roadmap_with_markers(self, lat: float, lng: float,
                                    route_id: str, turn_id: str,
                                    risk_level: str = "high",
                                    zoom: int = 17,
                                    force_download: bool = False) -> Optional[str]:
        """
        Download roadmap with risk marker for a specific location
        
        Args:
            lat: Latitude
            lng: Longitude
            route_id: Route identifier
            turn_id: Turn/spot identifier
            risk_level: Risk level for color coding
            zoom: Zoom level
            force_download: Force re-download even if file exists
        
        Returns:
            Path to saved image or None if failed
        """
        try:
            # Generate filename
            route_folder = self.get_route_image_folder(route_id)
            filename = f"roadmap_{turn_id}.png"
            filepath = route_folder / filename
            
            # Check if image already exists (CACHING LOGIC PRESERVED)
            if filepath.exists() and not force_download:
                logger.info(f"✅ Roadmap already exists: {filepath}")
                return str(filepath)
            
            # Get available API key
            api_key = self._get_available_key()
            if not api_key:
                logger.error("❌ No available API keys with remaining quota")
                return None
            
            # Rate limit before making API call
            self._rate_limit()
            
            # Determine marker color based on risk
            marker_colors = {
                'critical': 'red',
                'high': 'orange', 
                'medium': 'yellow',
                'low': 'green'
            }
            color = marker_colors.get(risk_level.lower(), 'red')
            
            params = {
                'center': f'{lat},{lng}',
                'zoom': zoom,
                'size': '640x480',
                'maptype': 'roadmap',
                'markers': f'color:{color}|size:large|{lat},{lng}',
                'key': api_key,
                'scale': 2
            }
            
            logger.info(f"Downloading roadmap for turn {turn_id}")
            response = requests.get(self.static_map_url, params=params, timeout=30)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Roadmap saved: {filepath}")
                
                # Log successful usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap',
                    success=True,
                    response_code=200,
                    image_type='roadmap',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    file_path=str(filepath)
                )
                
                return str(filepath)
            else:
                logger.error(f"Failed to download roadmap: {response.status_code}")
                
                # Log failed usage
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap',
                    success=False,
                    response_code=response.status_code,
                    image_type='roadmap',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    error_message=f"HTTP {response.status_code}"
                )
                
                return None
                
        except Exception as e:
            logger.error(f"Error downloading roadmap: {e}")
            if 'api_key' in locals():
                self._log_api_usage(
                    api_key=api_key,
                    request_type='staticmap',
                    success=False,
                    image_type='roadmap',
                    route_id=route_id,
                    turn_id=turn_id,
                    latitude=lat,
                    longitude=lng,
                    error_message=str(e)
                )
            return None
    
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
            'roadmap': (route_folder / f"roadmap_{turn_id}.png").exists()
        }
        
        status['all_exist'] = all([
            status['street_view'] or status['street_view_90'],  # At least one street view
            status['satellite'],
            status['roadmap']
        ])
        
        return status

    def download_turn_images(self, turn_data: Dict, route_id: str, 
                            force_download: bool = False) -> Dict[str, str]:
        """
        Download all images for a sharp turn
        
        Args:
            turn_data: Dictionary with turn information
            route_id: Route identifier
            force_download: Force re-download even if files exist
            
        Returns:
            Dictionary with paths to downloaded images
        """
        turn_id = str(turn_data.get('_id', ''))
        
        # Check existing images
        if not force_download:
            status = self.get_image_status(route_id, turn_id)
            logger.info(f"Image status for turn {turn_id}: {sum(status.values())}/6 images exist")
        
        lat = turn_data.get('latitude')
        lng = turn_data.get('longitude')
        risk_score = turn_data.get('riskScore', 5)
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
        
        # Download street view (with caching)
        street_view_path = self.download_street_view_image(
            lat, lng, route_id, turn_id, heading=heading, force_download=force_download
        )
        if street_view_path:
            images['street_view'] = street_view_path
        
        # Download satellite view (with caching)
        satellite_path = self.download_satellite_image(
            lat, lng, route_id, turn_id, zoom=18, force_download=force_download
        )
        if satellite_path:
            images['satellite'] = satellite_path
        
        # Download roadmap with marker (with caching)
        roadmap_path = self.download_roadmap_with_markers(
            lat, lng, route_id, turn_id, risk_level=risk_level, force_download=force_download
        )
        if roadmap_path:
            images['roadmap'] = roadmap_path
        
        return images

    def download_blind_spot_images(self, spot_data: Dict, route_id: str,
                                force_download: bool = False) -> Dict[str, str]:
        """
        Download all images for a blind spot
        
        Args:
            spot_data: Dictionary with blind spot information
            route_id: Route identifier
            force_download: Force re-download even if files exist
            
        Returns:
            Dictionary with paths to downloaded images
        """
        lat = spot_data.get('latitude')
        lng = spot_data.get('longitude')
        spot_id = str(spot_data.get('_id', ''))
        risk_score = spot_data.get('riskScore', 5)
        
        # Check existing images
        if not force_download:
            status = self.get_image_status(route_id, f"blind_{spot_id}")
            logger.info(f"Image status for blind spot {spot_id}: {sum(status.values())}/5 images exist")
        
        # Determine risk level
        if risk_score >= 8:
            risk_level = 'critical'
        elif risk_score >= 6:
            risk_level = 'high'
        elif risk_score >= 4:
            risk_level = 'medium'
        else:
            risk_level = 'low'
        
        images = {}
        
        # Download multiple street view angles for blind spots (with caching)
        for heading in [0, 90, 180, 270]:  # Four directions
            street_view_path = self.download_street_view_image(
                lat, lng, route_id, f"blind_{spot_id}", heading=heading, force_download=force_download
            )
            if street_view_path:
                images[f'street_view_{heading}'] = street_view_path
        
        # Download satellite view (with caching)
        satellite_path = self.download_satellite_image(
            lat, lng, route_id, f"blind_{spot_id}", zoom=19, force_download=force_download
        )
        if satellite_path:
            images['satellite'] = satellite_path
        
        return images
    
    def create_image_composite(self, images: List[str], output_path: str,
                             layout: str = 'horizontal') -> Optional[str]:
        """
        Create a composite image from multiple images
        
        Args:
            images: List of image paths
            output_path: Output file path
            layout: 'horizontal' or 'vertical'
            
        Returns:
            Path to composite image or None if failed
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
        import datetime
        
        route_folder = self.get_route_image_folder(route_id)
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        
        for file_path in route_folder.glob("*"):
            if file_path.is_file():
                if os.path.getmtime(file_path) < cutoff_time:
                    file_path.unlink()
                    logger.info(f"Deleted old image: {file_path}")
    
    def clear_route_cache(self, route_id: str):
        """
        Clear all cached images for a specific route
        """
        route_folder = self.get_route_image_folder(route_id)
        if route_folder.exists():
            import shutil
            shutil.rmtree(route_folder)
            logger.info(f"Cleared image cache for route {route_id}")
            route_folder.mkdir(parents=True, exist_ok=True)

    def get_cache_stats(self, route_id: str) -> Dict[str, Any]:
        """
        Get statistics about cached images for a route
        """
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
            'roadmap': 0
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
        
        return {
            'exists': True,
            'total_images': sum(image_types.values()),
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'image_types': image_types,
            'folder_path': str(route_folder)
        }
    
    # Additional methods for API key management (only used when multiple keys are configured)
    
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


# ============================================================================
# OPTIONAL EXAMPLE USAGE - FOR TESTING ONLY
# ============================================================================
if __name__ == "__main__":
    # Example: Multiple API keys can be provided
    api_keys = [
        os.getenv('GOOGLE_MAPS_API_KEY_1'),
        os.getenv('GOOGLE_MAPS_API_KEY_2'),
        os.getenv('GOOGLE_MAPS_API_KEY_3'),
    ]
    
    # Filter out None values
    api_keys = [k for k in api_keys if k]
    
    # Or load from comma-separated environment variable
    if not api_keys:
        multi_keys = os.getenv('GOOGLE_MAPS_API_KEYS', '')
        if multi_keys:
            api_keys = [k.strip() for k in multi_keys.split(',') if k.strip()]
    
    # For backward compatibility - also works with single key
    if not api_keys:
        single_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if single_key:
            api_keys = [single_key]
    
    downloader = GoogleMapsImageDownloader(api_key=api_keys if len(api_keys) > 1 else (api_keys[0] if api_keys else None))
    
    # Test download
    turn_data = {
        '_id': '12345',
        'latitude': 19.0760,
        'longitude': 72.8777,
        'riskScore': 8,
        'turnAngle': 95
    }
    
    images = downloader.download_turn_images(turn_data, 'test_route')
    print(f"Downloaded images: {images}")
    
    # Get usage statistics (only works with multiple keys)
    if len(api_keys) > 1:
        stats = downloader.get_usage_stats()
        print(f"API Usage Stats: {json.dumps(stats, indent=2)}")
        
        # Generate usage report
        report = downloader.get_usage_report()
        print(f"Usage Report: {json.dumps(report, indent=2)}")