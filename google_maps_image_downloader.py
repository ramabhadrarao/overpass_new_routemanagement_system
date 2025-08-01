#!/usr/bin/env python3
"""
Google Maps Image Downloader for HPCL Sharp Turns and Blind Spots
Downloads Street View and Satellite images for each critical point

IMPORTANT: This is a COMPATIBLE REPLACEMENT that maintains the same interface
but uses alternative APIs (Mapillary, Mapbox, Bing, OSM) instead of Google.
NO CHANGES NEEDED in files that import and use this class!
"""

import os
import requests
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any 
import logging
from PIL import Image
import io
import json
import math

logger = logging.getLogger(__name__)

class GoogleMapsImageDownloader:
    """
    Downloads and manages Google Maps images for sharp turns and blind spots
    
    COMPATIBILITY NOTE: This class maintains the exact same interface as the original
    GoogleMapsImageDownloader but uses alternative APIs internally.
    """
    
    def __init__(self, api_key: str, base_path: str = "./route_images"):
        """
        Initialize the image downloader
        
        Args:
            api_key: Google Maps API key (IGNORED - kept for compatibility)
            base_path: Base directory for storing images
        """
        # Note: api_key parameter is kept for compatibility but not used
        self.api_key = api_key  # Kept for compatibility
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # API endpoints - Using alternatives instead of Google
        self.street_view_url = "https://graph.mapillary.com"  # Mapillary instead
        self.static_map_url = "https://api.mapbox.com/styles/v1"  # Mapbox instead
        
        # Alternative API keys
        self.mapillary_token = os.getenv('MAPILLARY_ACCESS_TOKEN')
        self.mapbox_token = os.getenv('MAPBOX_ACCESS_TOKEN')
        self.bing_maps_key = os.getenv('BING_MAPS_KEY')
        
        # Additional endpoints
        self.osm_tile_server = "https://tile.openstreetmap.org"
        self.bing_static_api = "https://dev.virtualearth.net/REST/v1/Imagery/Map"
        
        # Rate limiting
        self.request_delay = 0.1  # 100ms between requests
        self.last_request_time = 0
        
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
        
        COMPATIBILITY: Same signature as original, but uses Mapillary instead of Google
        
        Args:
            lat: Latitude
            lng: Longitude
            route_id: Route identifier
            turn_id: Turn/spot identifier
            heading: Camera heading (IGNORED - Mapillary doesn't support this)
            fov: Field of view (IGNORED - for compatibility)
            pitch: Up/down angle (IGNORED - for compatibility)
            force_download: Force re-download even if file exists
        
        Returns:
            Path to saved image or None if failed
        """
        try:
            # Generate filename - SAME format as original for compatibility
            route_folder = self.get_route_image_folder(route_id)
            filename = f"streetview_{turn_id}_h{int(heading or 0)}.jpg"
            filepath = route_folder / filename
            
            # Check if image already exists
            if filepath.exists() and not force_download:
                logger.info(f"✅ Street view already exists: {filepath}")
                return str(filepath)
            
            # Try Mapillary first
            if self.mapillary_token:
                self._rate_limit()
                
                # Search for images near location
                search_url = f"{self.street_view_url}/images"
                params = {
                    'access_token': self.mapillary_token,
                    'fields': 'id,geometry,thumb_2048_url,captured_at,compass_angle',
                    'bbox': f'{lng-0.001},{lat-0.001},{lng+0.001},{lat+0.001}',
                    'limit': 10
                }
                
                response = requests.get(search_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('data') and len(data['data']) > 0:
                        # Get the closest image
                        closest_image = data['data'][0]
                        image_url = closest_image.get('thumb_2048_url')
                        
                        if image_url:
                            # Download the image
                            img_response = requests.get(image_url, timeout=30)
                            if img_response.status_code == 200:
                                with open(filepath, 'wb') as f:
                                    f.write(img_response.content)
                                logger.info(f"✅ Street view saved: {filepath}")
                                return str(filepath)
            
            # If no street view available, create a placeholder or return None
            logger.info(f"No street view available for turn {turn_id} at {lat},{lng}")
            return None
                
        except Exception as e:
            logger.error(f"Error downloading street view: {e}")
            return None
    
    def download_satellite_image(self, lat: float, lng: float, 
                           route_id: str, turn_id: str,
                           zoom: int = 18,
                           force_download: bool = False) -> Optional[str]:
        """
        Download satellite image for a specific location
        
        COMPATIBILITY: Same signature as original, but uses Mapbox/Bing instead of Google
        
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
            # Generate filename - SAME format as original for compatibility
            route_folder = self.get_route_image_folder(route_id)
            filename = f"satellite_{turn_id}.png"
            filepath = route_folder / filename
            
            # Check if image already exists
            if filepath.exists() and not force_download:
                logger.info(f"✅ Satellite view already exists: {filepath}")
                return str(filepath)
            
            self._rate_limit()
            
            # Try Mapbox first
            if self.mapbox_token:
                style_id = "mapbox/satellite-v9"
                url = f"{self.static_map_url}/{style_id}/static/{lng},{lat},{zoom},0/640x480@2x"
                
                params = {
                    'access_token': self.mapbox_token
                }
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    logger.info(f"✅ Satellite view saved: {filepath}")
                    return str(filepath)
            
            # Fallback to Bing Maps
            if self.bing_maps_key:
                url = f"{self.bing_static_api}/Aerial/{zoom}"
                params = {
                    'centerPoint': f'{lat},{lng}',
                    'mapSize': '640,480',
                    'key': self.bing_maps_key,
                    'format': 'jpeg'
                }
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    # Save as PNG to match expected format
                    img = Image.open(io.BytesIO(response.content))
                    img.save(filepath, 'PNG')
                    logger.info(f"✅ Satellite view saved: {filepath}")
                    return str(filepath)
            
            logger.error(f"Failed to download satellite view")
            return None
                
        except Exception as e:
            logger.error(f"Error downloading satellite view: {e}")
            return None

    def download_roadmap_with_markers(self, lat: float, lng: float,
                                    route_id: str, turn_id: str,
                                    risk_level: str = "high",
                                    zoom: int = 17,
                                    force_download: bool = False) -> Optional[str]:
        """
        Download roadmap with risk marker for a specific location
        
        COMPATIBILITY: Same signature as original, but uses Mapbox/OSM instead of Google
        
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
            # Generate filename - SAME format as original for compatibility
            route_folder = self.get_route_image_folder(route_id)
            filename = f"roadmap_{turn_id}.png"
            filepath = route_folder / filename
            
            # Check if image already exists
            if filepath.exists() and not force_download:
                logger.info(f"✅ Roadmap already exists: {filepath}")
                return str(filepath)
            
            self._rate_limit()
            
            # Determine marker color based on risk
            marker_colors = {
                'critical': 'f00',
                'high': 'f90', 
                'medium': 'ff0',
                'low': '0f0'
            }
            color = marker_colors.get(risk_level.lower(), 'f00')
            
            # Try Mapbox first
            if self.mapbox_token:
                marker = f"pin-l-danger+{color}({lng},{lat})"
                style_id = "mapbox/streets-v11"
                url = f"{self.static_map_url}/{style_id}/static/{marker}/{lng},{lat},{zoom},0/640x480@2x"
                
                params = {
                    'access_token': self.mapbox_token
                }
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    logger.info(f"✅ Roadmap saved: {filepath}")
                    return str(filepath)
            
            # Fallback to OSM
            return self._download_osm_fallback(lat, lng, route_folder, filename, zoom)
                
        except Exception as e:
            logger.error(f"Error downloading roadmap: {e}")
            return None
    
    def _download_osm_fallback(self, lat: float, lng: float, route_folder: Path, 
                              filename: str, zoom: int) -> Optional[str]:
        """Internal method to download OSM tile as fallback"""
        try:
            filepath = route_folder / filename
            
            # Convert lat/lon to tile numbers
            n = 2.0 ** zoom
            x = int((lng + 180.0) / 360.0 * n)
            y = int((1.0 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2.0 * n)
            
            # Download the tile
            tile_url = f"{self.osm_tile_server}/{zoom}/{x}/{y}.png"
            headers = {
                'User-Agent': 'HPCL-Journey-Risk-Management/1.0'
            }
            
            response = requests.get(tile_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                logger.info(f"✅ OSM fallback saved: {filepath}")
                return str(filepath)
                
        except Exception as e:
            logger.error(f"Error downloading OSM fallback: {e}")
            return None
    
    def get_image_status(self, route_id: str, turn_id: str) -> Dict[str, bool]:
        """
        Check which images already exist for a turn/spot
        
        COMPATIBILITY: Same return format as original
        
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
        
        COMPATIBILITY: Exact same signature and return format as original
        
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
        
        COMPATIBILITY: Exact same signature and return format as original
        
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
        
        # Download multiple street view angles for blind spots
        # Note: With Mapillary, we can't control angles, so we try nearby points
        for heading in [0, 90, 180, 270]:  # Four directions
            # Slight offset for each direction to get different viewpoints
            offset = 0.00005  # ~5 meters
            lat_offset = offset * math.cos(math.radians(heading))
            lng_offset = offset * math.sin(math.radians(heading))
            
            street_view_path = self.download_street_view_image(
                lat + lat_offset, lng + lng_offset, route_id, f"blind_{spot_id}", 
                heading=heading, force_download=force_download
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
        
        COMPATIBILITY: Exact same signature as original
        
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
        """Clean up old images for a route - COMPATIBILITY METHOD"""
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
        Clear all cached images for a specific route - COMPATIBILITY METHOD
        """
        route_folder = self.get_route_image_folder(route_id)
        if route_folder.exists():
            import shutil
            shutil.rmtree(route_folder)
            logger.info(f"Cleared image cache for route {route_id}")
            route_folder.mkdir(parents=True, exist_ok=True)

    def get_cache_stats(self, route_id: str) -> Dict[str, Any]:
        """
        Get statistics about cached images for a route - COMPATIBILITY METHOD
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


# ============================================================================
# BACKWARD COMPATIBILITY - These ensure the class works exactly like the original
# ============================================================================

if __name__ == "__main__":
    # This example shows that the interface is EXACTLY the same as the original
    
    # Initialize with "Google API key" (actually ignored, but interface is same)
    downloader = GoogleMapsImageDownloader(
        api_key="your_google_api_key_here",  # This is ignored internally
        base_path="./route_images"
    )
    
    # Example turn data - EXACTLY as used with original class
    turn_data = {
        '_id': '12345',
        'latitude': 28.6139,
        'longitude': 77.2090,
        'riskScore': 8,
        'turnAngle': 95
    }
    
    # Download images - EXACTLY same method call as original
    images = downloader.download_turn_images(turn_data, 'test_route')
    print(f"Downloaded images: {images}")
    
    # Check status - EXACTLY same method call as original
    status = downloader.get_image_status('test_route', '12345')
    print(f"Image status: {status}")
    
    # Get cache stats - EXACTLY same method call as original
    stats = downloader.get_cache_stats('test_route')
    print(f"Cache stats: {stats}")