#!/usr/bin/env python3
"""
Google Maps Image Downloader for HPCL Sharp Turns and Blind Spots
Downloads Street View and Satellite images for each critical point
Enhanced with TileServer GL support for satellite and roadmap images
"""

import os
import requests
import time
import math
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any 
import logging
from PIL import Image
import io

logger = logging.getLogger(__name__)

class GoogleMapsImageDownloader:
    """Downloads and manages Google Maps images for sharp turns and blind spots"""
    
    def __init__(self, api_key: str, base_path: str = "./route_images", 
                 tileserver_url: str = "http://69.62.73.201:8080", 
                 use_tileserver: bool = True):
        """
        Initialize the image downloader
        
        Args:
            api_key: Google Maps API key
            base_path: Base directory for storing images
            tileserver_url: TileServer GL URL (default: "http://69.62.73.201:8080")
            use_tileserver: Whether to use TileServer for satellite/roadmap images (default: True)
        """
        self.api_key = api_key
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # TileServer configuration
        self.tileserver_url = tileserver_url.rstrip('/') if tileserver_url else None
        self.use_tileserver = use_tileserver and tileserver_url is not None
        
        # API endpoints
        self.street_view_url = "https://maps.googleapis.com/maps/api/streetview"
        self.static_map_url = "https://maps.googleapis.com/maps/api/staticmap"
        
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
    
    def lat_lon_to_tile(self, lat: float, lon: float, zoom: int) -> Tuple[int, int]:
        """Convert latitude/longitude to tile coordinates"""
        lat_rad = math.radians(lat)
        n = 2.0 ** zoom
        x = int((lon + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x, y
    
    def download_tiles_and_stitch(self, lat: float, lng: float, zoom: int, 
                                  tile_type: str, size: Tuple[int, int] = (640, 480)) -> Optional[bytes]:
        """
        Download tiles from TileServer and stitch them together
        
        Args:
            lat: Latitude
            lng: Longitude
            zoom: Zoom level
            tile_type: 'india-satellite' or 'india'
            size: Desired output size (width, height)
            
        Returns:
            Stitched image as bytes or None
        """
        try:
            # Calculate center tile
            center_x, center_y = self.lat_lon_to_tile(lat, lng, zoom)
            
            # Calculate how many tiles we need (256px per tile)
            tiles_x = math.ceil(size[0] / 256)
            tiles_y = math.ceil(size[1] / 256)
            
            # Download tiles
            tiles = {}
            for dx in range(-(tiles_x//2), (tiles_x//2) + 1):
                for dy in range(-(tiles_y//2), (tiles_y//2) + 1):
                    tile_x = center_x + dx
                    tile_y = center_y + dy
                    
                    url = f"{self.tileserver_url}/data/{tile_type}/{zoom}/{tile_x}/{tile_y}.png"
                    
                    self._rate_limit()
                    response = requests.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        img = Image.open(io.BytesIO(response.content))
                        tiles[(dx, dy)] = img
                    else:
                        logger.warning(f"Failed to download tile {tile_x},{tile_y}: {response.status_code}")
            
            if not tiles:
                return None
            
            # Calculate canvas size
            canvas_width = tiles_x * 256
            canvas_height = tiles_y * 256
            
            # Create canvas and paste tiles
            canvas = Image.new('RGB', (canvas_width, canvas_height))
            
            for (dx, dy), tile_img in tiles.items():
                x = (dx + tiles_x//2) * 256
                y = (dy + tiles_y//2) * 256
                canvas.paste(tile_img, (x, y))
            
            # Crop to desired size from center
            left = (canvas_width - size[0]) // 2
            top = (canvas_height - size[1]) // 2
            right = left + size[0]
            bottom = top + size[1]
            
            cropped = canvas.crop((left, top, right, bottom))
            
            # Convert to bytes
            output = io.BytesIO()
            cropped.save(output, format='PNG')
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error downloading and stitching tiles: {e}")
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
        (Always uses Google Maps API)
        
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
            
            # Check if image already exists
            if filepath.exists() and not force_download:
                logger.info(f"✅ Street view already exists: {filepath}")
                return str(filepath)
            
            # Rate limit before making API call
            self._rate_limit()
            
            params = {
                'location': f'{lat},{lng}',
                'size': '640x480',
                'fov': fov,
                'pitch': pitch,
                'heading': heading,
                'key': self.api_key
            }
            
            logger.info(f"Downloading street view for turn {turn_id} at {lat},{lng}")
            response = requests.get(self.street_view_url, params=params, timeout=30)
            
            if response.status_code == 200:
                # Save image
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Street view saved: {filepath}")
                return str(filepath)
            else:
                logger.error(f"Failed to download street view: {response.status_code}")
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
        Uses TileServer if configured, otherwise Google Maps
        
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
            
            # Check if image already exists
            if filepath.exists() and not force_download:
                logger.info(f"✅ Satellite view already exists: {filepath}")
                return str(filepath)
            
            # Rate limit before making API call
            self._rate_limit()
            
            if self.use_tileserver:
                # Use TileServer GL
                logger.info(f"Downloading satellite view from TileServer for turn {turn_id}")
                image_data = self.download_tiles_and_stitch(
                    lat, lng, zoom, 'india-satellite', size=(640, 480)
                )
                
                if image_data:
                    with open(filepath, 'wb') as f:
                        f.write(image_data)
                    logger.info(f"✅ Satellite view saved from TileServer: {filepath}")
                    return str(filepath)
                else:
                    logger.error("Failed to download from TileServer")
                    # Fall back to Google Maps if TileServer fails
                    
            # Use Google Maps API (fallback or default)
            params = {
                'center': f'{lat},{lng}',
                'zoom': zoom,
                'size': '640x480',
                'maptype': 'satellite',
                'key': self.api_key,
                'scale': 2  # Higher quality
            }
            
            logger.info(f"Downloading satellite view from Google Maps for turn {turn_id}")
            response = requests.get(self.static_map_url, params=params, timeout=30)
            
            if response.status_code == 200:
                # Save image
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Satellite view saved: {filepath}")
                return str(filepath)
            else:
                logger.error(f"Failed to download satellite view: {response.status_code}")
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
        Uses TileServer if configured, otherwise Google Maps
        
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
            
            # Check if image already exists
            if filepath.exists() and not force_download:
                logger.info(f"✅ Roadmap already exists: {filepath}")
                return str(filepath)
            
            # Rate limit before making API call
            self._rate_limit()
            
            if self.use_tileserver:
                # Use TileServer GL
                logger.info(f"Downloading roadmap from TileServer for turn {turn_id}")
                image_data = self.download_tiles_and_stitch(
                    lat, lng, zoom, 'india', size=(640, 480)
                )
                
                if image_data:
                    # Add marker overlay on the image
                    img = Image.open(io.BytesIO(image_data))
                    
                    # TODO: Add marker drawing logic here if needed
                    # For now, just save the base map
                    
                    with open(filepath, 'wb') as f:
                        img.save(f, 'PNG')
                    logger.info(f"✅ Roadmap saved from TileServer: {filepath}")
                    return str(filepath)
                else:
                    logger.error("Failed to download from TileServer")
                    # Fall back to Google Maps if TileServer fails
            
            # Use Google Maps API (fallback or default)
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
                'key': self.api_key,
                'scale': 2
            }
            
            logger.info(f"Downloading roadmap from Google Maps for turn {turn_id}")
            response = requests.get(self.static_map_url, params=params, timeout=30)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                logger.info(f"✅ Roadmap saved: {filepath}")
                return str(filepath)
            else:
                logger.error(f"Failed to download roadmap: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading roadmap: {e}")
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
    
    # Additional helper method to clear cache for specific route
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

    # Method to get cache statistics
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

# Example usage:
if __name__ == "__main__":
    # Initialize with TileServer support
    downloader = GoogleMapsImageDownloader(
        api_key="YOUR_GOOGLE_API_KEY",
        tileserver_url="http://69.62.73.201:8080",
        use_tileserver=True  # Enable TileServer for satellite/roadmap
    )
    
    # Download images for a turn
    turn_data = {
        '_id': '12345',
        'latitude': 19.0760,
        'longitude': 72.8777,
        'riskScore': 8,
        'turnAngle': 95
    }
    
    images = downloader.download_turn_images(turn_data, 'test_route')
    print(f"Downloaded images: {images}")