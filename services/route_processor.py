# services/route_processor.py
# Optimized version with batch processing for emergency services
# Path: /services/route_processor.py

import os
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from bson import ObjectId
from services.overpass_service import OverpassService
from services.risk_calculator import RiskCalculator
from utils.file_parser import FileParser
from models import *
import logging
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import hashlib
import traceback
import numpy as np

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

class RouteProcessor:
    def __init__(self, db, overpass_url):
        logger.info("Initializing RouteProcessor")
        self.db = db
        self.overpass_service = OverpassService(overpass_url, db)
        self.risk_calculator = RiskCalculator()
        self.file_parser = FileParser()
        
        # Initialize models
        self.route_model = Route(db)
        self.sharp_turn_model = SharpTurn(db)
        self.blind_spot_model = BlindSpot(db)
        self.accident_prone_model = AccidentProneArea(db)
        self.emergency_service_model = EmergencyService(db)
        self.network_coverage_model = NetworkCoverage(db)
        self.road_condition_model = RoadCondition(db)
        self.eco_zone_model = EcoSensitiveZone(db)
        self.weather_condition_model = WeatherCondition(db)
        self.traffic_data_model = TrafficData(db)
        self.api_log_model = APILog(db)
        
        # Load API keys
        self.era5_api_key = "h4DSeoxB88OwRw7rh42sWJlx8BphPHCi"
        self.era5_api_url = "http://43.250.40.133:6000/api/weather/point"
        self.tomtom_api_key = os.getenv('TOMTOM_API_KEY', '4GMXpCknsEI6v22oQlZe5CFlV1Ev0xQu')
        self.here_api_key = os.getenv('HERE_API_KEY', '_Zmq3222RvY4Y5XspG6X4RQbOx2-QIp0C171cD3BHls')
        self.mapbox_api_key = os.getenv('MAPBOX_API_KEY', 'pk.eyJ1IjoiYW5pbDI1IiwiYSI6ImNtYmtlanhpYjBwZW4ya3F4ZnZ2NmNxNDkifQ.N0WsW5T60dxrG80rhnee0g')
        
        # Cache settings
        self.cache_enabled = True
        self.cache_expiry_hours = 24
        
        # Thread pool for concurrent processing
        self.executor = ThreadPoolExecutor(max_workers=3)
        
        # API rate limits (requests per second)
        self.rate_limits = {
            'era5': 1.0,
            'tomtom': 2.0,
            'here': 2.0,
            'overpass': 1.0,
            'mapbox': 2.0
        }
        self.last_api_call = {}
        
        logger.info("RouteProcessor initialized successfully")
    
    def _get_cache_key(self, cache_type: str, lat: float, lng: float) -> str:
        """Generate cache key for API results"""
        return hashlib.md5(f"{cache_type}_{lat:.4f}_{lng:.4f}".encode()).hexdigest()
    
    def _get_cached_data(self, cache_type: str, lat: float, lng: float) -> Optional[Dict]:
        """Get cached API data if available and not expired"""
        if not self.cache_enabled:
            return None
            
        cache_key = self._get_cache_key(cache_type, lat, lng)
        try:
            cached = self.db.api_cache.find_one({
                'cache_key': cache_key,
                'cache_type': cache_type
            })
            
            if cached:
                if cached['expires_at'] > datetime.utcnow():
                    logger.debug(f"Cache HIT for {cache_type} at {lat:.4f},{lng:.4f}")
                    return cached['data']
                else:
                    logger.debug(f"Cache EXPIRED for {cache_type} at {lat:.4f},{lng:.4f}")
            else:
                logger.debug(f"Cache MISS for {cache_type} at {lat:.4f},{lng:.4f}")
        except Exception as e:
            logger.error(f"Cache lookup error: {e}")
            
        return None
    
    def _save_to_cache(self, cache_type: str, lat: float, lng: float, data: Dict):
        """Save API response to cache"""
        if not self.cache_enabled:
            return
            
        cache_key = self._get_cache_key(cache_type, lat, lng)
        try:
            self.db.api_cache.update_one(
                {'cache_key': cache_key},
                {
                    '$set': {
                        'cache_type': cache_type,
                        'cache_key': cache_key,
                        'data': data,
                        'created_at': datetime.utcnow(),
                        'expires_at': datetime.utcnow() + timedelta(hours=self.cache_expiry_hours)
                    }
                },
                upsert=True
            )
            logger.debug(f"Saved to cache: {cache_type} at {lat:.4f},{lng:.4f}")
        except Exception as e:
            logger.error(f"Cache save error: {e}")
    
    def _rate_limit_api(self, api_name: str):
        """Implement rate limiting for API calls"""
        if api_name not in self.rate_limits:
            return
            
        min_interval = 1.0 / self.rate_limits[api_name]
        
        if api_name in self.last_api_call:
            elapsed = time.time() - self.last_api_call[api_name]
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                logger.debug(f"Rate limiting {api_name}: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
                
        self.last_api_call[api_name] = time.time()
    
    def process_single_route_fast(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Fast processing mode with batched API calls and caching"""
        logger.info(f"=== Starting fast processing for route {route_id} ===")
        logger.info(f"Route: {route_info['BU Code']} to {route_info['Row Labels']}")
        logger.info(f"Coordinates: {len(coordinates)} points")
        
        start_time = time.time()
        self.route_model.update_processing_status(route_id, 'processing')
        
        try:
            # Calculate total route distance
            logger.debug("Calculating route distance...")
            total_distance = self._calculate_total_distance(coordinates)
            logger.info(f"Route {route_id} total distance: {total_distance} km")
            
            # Process each component sequentially for better debugging
            results = {}
            
            # 1. Geometric analysis (no API calls needed)
            logger.info("STEP 1/6: Analyzing geometry...")
            try:
                geometry_start = time.time()
                geometry_result = self._analyze_geometry_fast(route_id, coordinates, total_distance)
                results.update(geometry_result)
                logger.info(f"Geometry analysis completed in {time.time() - geometry_start:.2f}s")
            except Exception as e:
                logger.error(f"Geometry analysis failed: {e}")
                logger.error(traceback.format_exc())
            
            # 2. Emergency services - OPTIMIZED with batch processing
            logger.info("STEP 2/6: Getting emergency services...")
            try:
                emergency_start = time.time()
                emergency_result = self._get_emergency_services_batch(route_id, coordinates)
                results.update(emergency_result)
                logger.info(f"Emergency services completed in {time.time() - emergency_start:.2f}s")
            except Exception as e:
                logger.error(f"Emergency services failed: {e}")
                logger.error(traceback.format_exc())
            
            # 3. Road conditions
            logger.info("STEP 3/6: Analyzing road conditions...")
            try:
                road_start = time.time()
                road_result = self._analyze_road_conditions_fast(route_id, coordinates)
                results.update(road_result)
                logger.info(f"Road conditions completed in {time.time() - road_start:.2f}s")
            except Exception as e:
                logger.error(f"Road conditions failed: {e}")
                logger.error(traceback.format_exc())
            
            # 4. Network coverage
            logger.info("STEP 4/6: Analyzing network coverage...")
            try:
                network_start = time.time()
                network_result = self._analyze_network_coverage_fast(route_id, coordinates, total_distance)
                results.update(network_result)
                logger.info(f"Network coverage completed in {time.time() - network_start:.2f}s")
            except Exception as e:
                logger.error(f"Network coverage failed: {e}")
                logger.error(traceback.format_exc())
            
            # 5. Weather data (ERA5 API)
            logger.info("STEP 5/6: Getting weather data...")
            try:
                weather_start = time.time()
                weather_result = self._get_weather_data_fast(route_id, coordinates)
                results.update(weather_result)
                logger.info(f"Weather data completed in {time.time() - weather_start:.2f}s")
            except Exception as e:
                logger.error(f"Weather data failed: {e}")
                logger.error(traceback.format_exc())
            
            # 6. Traffic data (TomTom API)
            logger.info("STEP 6/6: Getting traffic data...")
            try:
                traffic_start = time.time()
                traffic_result = self._get_traffic_data_fast(route_id, coordinates)
                results.update(traffic_result)
                logger.info(f"Traffic data completed in {time.time() - traffic_start:.2f}s")
            except Exception as e:
                logger.error(f"Traffic data failed: {e}")
                logger.error(traceback.format_exc())
            
            # Calculate risk scores
            logger.info("Calculating risk scores...")
            risk_scores = self.risk_calculator.calculate_overall_risk_score(results)
            
            # Update route with risk scores
            risk_scores_with_level = risk_scores['scores'].copy()
            risk_scores_with_level['overall'] = risk_scores['overall']
            risk_scores_with_level['risk_level'] = risk_scores['risk_level']
            
            self.route_model.update_risk_scores(route_id, risk_scores_with_level)
            self.route_model.update_processing_status(route_id, 'completed')
            
            total_time = time.time() - start_time
            logger.info(f"=== Route {route_id} processed successfully in {total_time:.2f}s ===")
            logger.info(f"Risk Level: {risk_scores['risk_level']} (Score: {risk_scores['overall']})")
            
            # Log summary of API calls
            api_logs = list(self.db.api_logs.find({'route_id': ObjectId(route_id)}))
            logger.info(f"Total API calls made: {len(api_logs)}")
            for api in ['overpass', 'era5_weather', 'tomtom_traffic']:
                count = len([log for log in api_logs if log['api_name'] == api])
                logger.info(f"  - {api}: {count} calls")
            
            return {
                'route_id': route_id,
                'status': 'completed',
                'risk_level': risk_scores['risk_level'],
                'overall_score': risk_scores['overall']
            }
            
        except Exception as e:
            logger.error(f"=== Route processing FAILED for {route_id} ===")
            logger.error(f"Error: {str(e)}")
            logger.error(traceback.format_exc())
            self.route_model.update_processing_status(route_id, 'failed', str(e))
            raise e
    
    def _get_emergency_services_batch(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Optimized batch processing for emergency services"""
        logger.debug(f"Getting emergency services for route {route_id}")
        bounds = self._calculate_bounds(coordinates)
        logger.debug(f"Route bounds: {bounds}")
        
        try:
            # Check cache first
            cached_data = self._get_cached_data('emergency_services', bounds['min_lat'], bounds['min_lng'])
            
            if cached_data:
                emergency_services = cached_data
                logger.info(f"Using CACHED emergency services data ({len(emergency_services)} services)")
            else:
                logger.info("Calling Overpass API for emergency services...")
                self._rate_limit_api('overpass')
                emergency_services = self.overpass_service.get_emergency_services(route_id, bounds)
                self._save_to_cache('emergency_services', bounds['min_lat'], bounds['min_lng'], emergency_services)
                logger.info(f"Fetched {len(emergency_services)} emergency services from Overpass API")
            
            # BATCH PROCESS emergency services
            logger.debug("Batch processing emergency services...")
            
            # Pre-calculate all route segments for efficient distance calculation
            route_segments = []
            for i in range(len(coordinates) - 1):
                route_segments.append({
                    'start': coordinates[i],
                    'end': coordinates[i + 1],
                    'cumulative_distance': self._calculate_cumulative_distance(coordinates, i)
                })
            
            # Process services in batches of 50
            batch_size = 50
            processed_services = []
            
            for batch_start in range(0, len(emergency_services), batch_size):
                batch_end = min(batch_start + batch_size, len(emergency_services))
                batch = emergency_services[batch_start:batch_end]
                
                logger.debug(f"Processing batch {batch_start//batch_size + 1}/{(len(emergency_services) + batch_size - 1)//batch_size}")
                
                # Process batch
                batch_results = []
                for service in batch:
                    # Quick distance calculation using vectorized approach
                    service['distance_from_route_km'] = self._quick_distance_to_route(
                        service['latitude'], service['longitude'], coordinates
                    )
                    service['distance_from_start_km'] = self._quick_distance_along_route(
                        service['latitude'], service['longitude'], route_segments
                    )
                    service['distance_from_end_km'] = self._calculate_total_distance(coordinates) - service['distance_from_start_km']
                    service = self._enrich_emergency_service(service)
                    batch_results.append(service)
                
                # Bulk insert the batch
                if batch_results:
                    for service in batch_results:
                        self.emergency_service_model.create_emergency_service(route_id, service)
                    processed_services.extend(batch_results)
                
                logger.debug(f"Processed {len(processed_services)}/{len(emergency_services)} services")
            
            logger.debug(f"Emergency services batch processing complete")
            
        except Exception as e:
            logger.error(f"Error getting emergency services: {e}")
            logger.error(traceback.format_exc())
            emergency_services = []
            
        return {'emergency_services': emergency_services}
    
    def _quick_distance_to_route(self, lat: float, lng: float, route_points: List[Dict]) -> float:
        """Quick distance calculation using numpy if available, otherwise fallback"""
        try:
            # Use numpy for vectorized calculation if available
            import numpy as np
            
            # Convert to arrays
            point_lats = np.array([p['latitude'] for p in route_points])
            point_lngs = np.array([p['longitude'] for p in route_points])
            
            # Haversine formula vectorized
            lat_diff = np.radians(point_lats - lat)
            lng_diff = np.radians(point_lngs - lng)
            
            a = np.sin(lat_diff/2)**2 + np.cos(np.radians(lat)) * \
                np.cos(np.radians(point_lats)) * np.sin(lng_diff/2)**2
            c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
            distances = 6371 * c  # Earth radius in km
            
            return round(float(np.min(distances)), 2)
            
        except ImportError:
            # Fallback to original method
            return self._calculate_distance_to_route(lat, lng, route_points)
    
    def _quick_distance_along_route(self, lat: float, lng: float, route_segments: List[Dict]) -> float:
        """Quick distance along route calculation"""
        min_distance = float('inf')
        distance_along_route = 0
        
        for segment in route_segments:
            distance = self.risk_calculator.calculate_distance(
                lat, lng, 
                segment['start']['latitude'], 
                segment['start']['longitude']
            )
            
            if distance < min_distance:
                min_distance = distance
                distance_along_route = segment['cumulative_distance']
        
        return round(distance_along_route, 2)
    
    def _analyze_geometry_fast(self, route_id: str, coordinates: List[Dict], total_distance: float) -> Dict:
        """Fast geometric analysis without API calls"""
        logger.debug(f"Analyzing geometry for route {route_id}")
        results = {}
        
        try:
            # Analyze sharp turns
            logger.debug("Analyzing sharp turns...")
            sharp_turns = self.risk_calculator.analyze_sharp_turns(coordinates)
            logger.debug(f"Found {len(sharp_turns)} sharp turns")
            
            for turn in sharp_turns:
                turn['distance_from_end_km'] = total_distance - turn['distance_from_start_km']
                turn['turn_severity'] = self._get_turn_severity(turn['turn_angle'])
                turn['road_surface'] = 'good'
                turn['guardrails'] = False
                turn['warning_signs'] = False
                self.sharp_turn_model.create_sharp_turn(route_id, turn)
            results['sharp_turns'] = sharp_turns
            
            # Identify blind spots
            logger.debug("Identifying blind spots...")
            blind_spots = self.risk_calculator.identify_blind_spots(coordinates)
            logger.debug(f"Found {len(blind_spots)} blind spots")
            
            for spot in blind_spots:
                spot['distance_from_end_km'] = total_distance - spot['distance_from_start_km']
                spot['gradient'] = 0
                spot['curvature'] = 0
                spot['road_width'] = 7
                self.blind_spot_model.create_blind_spot(route_id, spot)
            results['blind_spots'] = blind_spots
            
            # Identify accident prone areas
            logger.debug("Identifying accident prone areas...")
            accident_areas = self._identify_accident_prone_areas(sharp_turns, [])
            logger.debug(f"Found {len(accident_areas)} accident prone areas")
            
            for area in accident_areas:
                area['distance_from_end_km'] = total_distance - area['distance_from_start_km']
                area = self._enrich_accident_area(area)
                self.accident_prone_model.create_accident_area(route_id, area)
            results['accident_prone_areas'] = accident_areas
            
            logger.debug(f"Geometry analysis complete: {len(sharp_turns)} turns, {len(blind_spots)} blind spots, {len(accident_areas)} accident areas")
            
        except Exception as e:
            logger.error(f"Error in geometry analysis: {e}")
            logger.error(traceback.format_exc())
            # Return empty results on error
            results = {
                'sharp_turns': [],
                'blind_spots': [],
                'accident_prone_areas': []
            }
            
        return results
    
    def _analyze_road_conditions_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Analyze road conditions with batched requests"""
        logger.debug(f"Analyzing road conditions for route {route_id}")
        road_conditions = []
        
        try:
            # Sample fewer points for faster processing
            sample_interval = max(1, len(coordinates) // 10)
            sample_points = coordinates[::sample_interval]
            logger.debug(f"Sampling {len(sample_points)} points from {len(coordinates)} total")
            
            for i, point in enumerate(sample_points):
                # Check cache
                cached = self._get_cached_data('road_condition', point['latitude'], point['longitude'])
                
                if cached:
                    condition = cached
                    logger.debug(f"Using cached road condition for point {i+1}/{len(sample_points)}")
                else:
                    # Simple road condition based on location
                    condition = {
                        'latitude': point['latitude'],
                        'longitude': point['longitude'],
                        'road_type': 'highway',
                        'surface': 'asphalt',
                        'lanes': 2,
                        'max_speed': 60,
                        'width': 7,
                        'under_construction': False,
                        'surface_quality': 'good',
                        'risk_score': 3,
                        'distance_from_start_km': self._calculate_cumulative_distance(coordinates, i * sample_interval),
                        'has_potholes': False,
                        'data_source': 'ESTIMATED'
                    }
                    self._save_to_cache('road_condition', point['latitude'], point['longitude'], condition)
                
                self.road_condition_model.create_road_condition(route_id, condition)
                road_conditions.append(condition)
            
            logger.debug(f"Road conditions analysis complete: {len(road_conditions)} conditions")
            
        except Exception as e:
            logger.error(f"Error analyzing road conditions: {e}")
            logger.error(traceback.format_exc())
            
        return {'road_conditions': road_conditions}
    
    def _analyze_network_coverage_fast(self, route_id: str, coordinates: List[Dict], total_distance: float) -> Dict:
        """Fast network coverage analysis"""
        logger.debug(f"Analyzing network coverage for route {route_id}")
        
        try:
            network_coverage = self.risk_calculator.analyze_network_coverage(coordinates)
            logger.debug(f"Generated {len(network_coverage)} network coverage points")
            
            for coverage in network_coverage:
                coverage['distance_from_end_km'] = total_distance - coverage['distance_from_start_km']
                self.network_coverage_model.create_network_coverage(route_id, coverage)
            
            dead_zones = [c for c in network_coverage if c['is_dead_zone']]
            logger.debug(f"Network coverage complete: {len(network_coverage)} points, {len(dead_zones)} dead zones")
            
        except Exception as e:
            logger.error(f"Error analyzing network coverage: {e}")
            logger.error(traceback.format_exc())
            network_coverage = []
            
        return {'network_coverage': network_coverage}
    
    # Complete weather-related methods update for route_processor.py
    # Replace all weather-related methods with these

    def _get_weather_data_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Get weather data using seasonal API with better error handling"""
        logger.debug(f"Getting weather data for route {route_id}")
        weather_conditions = []
        
        try:
            # Sample points along the route
            total_points = len(coordinates)
            if total_points <= 3:
                sample_indices = list(range(total_points))
            else:
                # Sample up to 5 points: start, 1/4, middle, 3/4, end
                sample_indices = [
                    0,
                    total_points // 4,
                    total_points // 2,
                    3 * total_points // 4,
                    total_points - 1
                ]
            
            logger.info(f"Sampling {len(sample_indices)} points for weather data")
            
            # Try seasonal API first
            seasonal_success = False
            try:
                # Prepare coordinates for seasonal API
                api_coordinates = []
                for idx in sample_indices:
                    point = coordinates[idx]
                    api_coordinates.append({
                        "latitude": point['latitude'],
                        "longitude": point['longitude'],
                        "name": f"Point_{idx}"
                    })
                
                # Check cache
                cache_key = f"seasonal_weather_{coordinates[0]['latitude']}_{coordinates[0]['longitude']}"
                cached_data = self._get_cached_data('seasonal_weather', coordinates[0]['latitude'], coordinates[0]['longitude'])
                
                if cached_data:
                    logger.info("Using CACHED seasonal weather data")
                    seasonal_data = cached_data
                else:
                    logger.info(f"Calling ERA5 seasonal API for {len(api_coordinates)} points")
                    self._rate_limit_api('era5')
                    seasonal_data = self._get_era5_seasonal_weather(api_coordinates, route_id)
                    
                    if seasonal_data and len(seasonal_data) > 0:
                        self._save_to_cache('seasonal_weather', coordinates[0]['latitude'], 
                                        coordinates[0]['longitude'], seasonal_data)
                
                if seasonal_data and len(seasonal_data) > 0:
                    # Process seasonal data
                    weather_conditions = self._process_seasonal_weather_data(
                        seasonal_data, sample_indices, coordinates, route_id
                    )
                    seasonal_success = True
                    logger.info(f"Successfully processed {len(weather_conditions)} weather conditions from seasonal API")
                else:
                    logger.warning("Seasonal API returned no valid data")
                    
            except Exception as e:
                logger.error(f"Seasonal API failed: {e}")
                seasonal_success = False
            
            # If seasonal API failed or returned no data, try single-point API
            if not seasonal_success or len(weather_conditions) == 0:
                logger.warning("Falling back to single-point weather API")
                
                for i, idx in enumerate(sample_indices[:3]):  # Limit to 3 points for single-point API
                    point = coordinates[idx]
                    logger.debug(f"Getting weather for point {i+1}/3 at {point['latitude']}, {point['longitude']}")
                    
                    # Check cache
                    cached = self._get_cached_data('weather', point['latitude'], point['longitude'])
                    
                    if cached:
                        weather = cached
                        weather['distance_from_start_km'] = self._calculate_cumulative_distance(coordinates, idx)
                        logger.debug(f"Using cached weather for point {i+1}")
                    else:
                        # Call single-point API
                        self._rate_limit_api('era5')
                        weather = self._get_era5_weather(point, coordinates, idx, route_id)
                        
                        if weather and weather.get('data_available', False):
                            self._save_to_cache('weather', point['latitude'], point['longitude'], weather)
                        else:
                            logger.warning(f"No weather data available for point {point['latitude']}, {point['longitude']}")
                            # Use simulated weather data
                            weather = self._get_simulated_weather(point, coordinates, idx)
                    
                    if weather:
                        # Ensure all required fields are present
                        weather = self._ensure_weather_fields(weather)
                        self.weather_condition_model.create_weather_condition(route_id, weather)
                        weather_conditions.append(weather)
            
            # If still no weather data, create simulated data
            if len(weather_conditions) == 0:
                logger.warning("No weather data available from APIs, using simulated data")
                for i, idx in enumerate(sample_indices[:3]):
                    point = coordinates[idx]
                    weather = self._get_simulated_weather(point, coordinates, idx)
                    weather = self._ensure_weather_fields(weather)
                    self.weather_condition_model.create_weather_condition(route_id, weather)
                    weather_conditions.append(weather)
            
            logger.info(f"Weather data collection complete: {len(weather_conditions)} conditions")
            
        except Exception as e:
            logger.error(f"Error getting weather data: {e}")
            logger.error(traceback.format_exc())
            # Return at least one simulated weather point
            if len(coordinates) > 0:
                weather = self._get_simulated_weather(coordinates[0], coordinates, 0)
                weather = self._ensure_weather_fields(weather)
                self.weather_condition_model.create_weather_condition(route_id, weather)
                weather_conditions = [weather]
        
        return {'weather_conditions': weather_conditions}
    def _ensure_weather_fields(self, weather: Dict) -> Dict:
        """Ensure all required fields are present in weather data for PDF compatibility"""
        # Map any alternative field names to expected names
        field_mappings = {
            'weatherCondition': 'weather_condition',
            'averageTemperature': 'average_temperature',
            'visibilityKm': 'visibility_km',
            'windSpeedKmph': 'wind_speed_kmph',
            'windDirection': 'wind_direction',
            'precipitationMm': 'precipitation_mm',
            'roadSurfaceCondition': 'road_surface_condition',
            'riskScore': 'risk_score',
            'monsoonRisk': 'monsoon_risk',
            'drivingConditionImpact': 'driving_condition_impact'
        }
        
        # Create normalized weather data
        normalized = {}
        
        # Copy existing fields
        for key, value in weather.items():
            normalized[key] = value
        
        # Add missing fields with defaults
        defaults = {
            'season': 'summer',
            'weather_condition': 'clear',
            'average_temperature': 25,
            'humidity': 60,
            'pressure': 1013,
            'visibility_km': 10,
            'wind_speed_kmph': 10,
            'wind_direction': 'N',
            'precipitation_mm': 0,
            'road_surface_condition': 'dry',
            'risk_score': 3,
            'monsoon_risk': 3,
            'driving_condition_impact': 'minimal',
            'uv_index': 5,
            'extreme_weather_history': [],
            'recommended_precautions': [],
            'data_source': 'UNKNOWN',
            'data_year': datetime.now().year,
            'forecast_accuracy': 70
        }
        
        for field, default_value in defaults.items():
            if field not in normalized:
                # Check if camelCase version exists
                camel_case = ''.join(word.capitalize() if i > 0 else word 
                                for i, word in enumerate(field.split('_')))
                if camel_case in weather:
                    normalized[field] = weather[camel_case]
                else:
                    normalized[field] = default_value
        
        # Also add camelCase versions for PDF compatibility
        for camel, snake in field_mappings.items():
            if snake in normalized and camel not in normalized:
                normalized[camel] = normalized[snake]
        
        # Add recommended precautions if not present
        if not normalized.get('recommended_precautions'):
            normalized['recommended_precautions'] = self._get_weather_precautions(normalized['season'])
        
        return normalized
    def _get_era5_seasonal_weather(self, coordinates: List[Dict], route_id: str) -> Optional[List[Dict]]:
        """Get seasonal weather from ERA5 API - updated to handle all seasons"""
        try:
            url = "http://43.250.40.133:6000/api/weather/route/seasonal"
            
            payload = {
                "coordinates": coordinates
            }
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-Key': self.era5_api_key
            }
            
            logger.debug(f"ERA5 seasonal API request to {url}")
            logger.debug(f"Request coordinates: {[(c['latitude'], c['longitude']) for c in coordinates]}")
            
            start_time = time.time()
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response_time = (time.time() - start_time) * 1000
            
            logger.info(f"ERA5 seasonal API response: {response.status_code} in {response_time:.0f}ms")
            
            # Log API call
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='era5_seasonal_weather',
                endpoint=url,
                request_data={'coordinates_count': len(coordinates)},
                response_data={'status_code': response.status_code, 'response_time_ms': response_time},
                status_code=response.status_code,
                response_time=response_time
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"Seasonal API response structure: {type(data)}")
                
                # Log the structure to understand what we're getting
                if isinstance(data, list) and len(data) > 0:
                    logger.debug(f"Response has {len(data)} items")
                    logger.debug(f"First item keys: {data[0].keys() if isinstance(data[0], dict) else 'Not a dict'}")
                
                return data
            else:
                logger.error(f"ERA5 seasonal API error: {response.status_code} - {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("ERA5 seasonal API timeout after 30 seconds")
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='era5_seasonal_weather',
                endpoint=url,
                request_data={'coordinates_count': len(coordinates)},
                response_data=None,
                status_code=0,
                response_time=30000,
                error='Timeout'
            )
            return None
        except Exception as e:
            logger.error(f"Error calling ERA5 seasonal API: {e}")
            logger.error(traceback.format_exc())
            return None
    def _process_seasonal_weather_data(self, seasonal_data: List[Dict], sample_indices: List[int], 
                                coordinates: List[Dict], route_id: str) -> List[Dict]:
        """Process seasonal weather data for ALL seasons"""
        weather_conditions = []
        total_distance = self._calculate_total_distance(coordinates)
        
        # Process response based on its structure
        if isinstance(seasonal_data, list) and len(seasonal_data) > 0:
            # Check if it's already separated by seasons
            first_item = seasonal_data[0]
            
            if isinstance(first_item, dict) and 'season' in first_item and 'route_weather' in first_item:
                # Format: [{"season": "summer", "route_weather": [...]}, ...]
                logger.info("Processing seasonal data with explicit seasons")
                
                for season_data in seasonal_data:
                    season = season_data['season']
                    route_weather = season_data.get('route_weather', [])
                    
                    logger.info(f"Processing {season} season with {len(route_weather)} points")
                    
                    for point_idx, weather_point in enumerate(route_weather):
                        if point_idx < len(sample_indices):
                            sample_idx = sample_indices[point_idx]
                            coord = coordinates[sample_idx]
                            
                            weather_condition = self._create_weather_condition(
                                weather_point, coord, sample_idx, coordinates, 
                                total_distance, season, route_id
                            )
                            
                            if weather_condition:
                                weather_conditions.append(weather_condition)
            
            else:
                # Format: Direct list of weather points
                # We need to create entries for each season
                logger.info("Processing direct weather data - creating all seasons")
                
                seasons = ['summer', 'monsoon', 'autumn', 'winter']
                
                for season in seasons:
                    logger.info(f"Creating weather data for {season} season")
                    
                    for i, weather_point in enumerate(seasonal_data):
                        if i < len(sample_indices):
                            sample_idx = sample_indices[i]
                            coord = coordinates[sample_idx]
                            
                            # Adjust weather data based on season
                            adjusted_weather = self._adjust_weather_for_season(weather_point, season)
                            
                            weather_condition = self._create_weather_condition(
                                adjusted_weather, coord, sample_idx, coordinates, 
                                total_distance, season, route_id
                            )
                            
                            if weather_condition:
                                weather_conditions.append(weather_condition)
        
        logger.info(f"Total weather conditions created: {len(weather_conditions)}")
        return weather_conditions

    def _create_weather_condition(self, weather_point: Dict, coord: Dict, sample_idx: int, 
                                coordinates: List[Dict], total_distance: float, 
                                season: str, route_id: str) -> Optional[Dict]:
        """Create a weather condition entry with all required fields"""
        
        if not weather_point.get('data_available', True):
            logger.warning(f"Skipping weather point - data not available")
            return None
        
        cumulative_distance = self._calculate_cumulative_distance(coordinates, sample_idx)
        
        # Create weather condition with all required fields
        weather_condition = {
            'latitude': coord['latitude'],
            'longitude': coord['longitude'],
            'distance_from_start_km': cumulative_distance,
            'distance_from_end_km': total_distance - cumulative_distance,
            
            # Season is explicitly set
            'season': season,
            
            # Weather data - handle both snake_case and camelCase
            'weather_condition': weather_point.get('weather_condition') or weather_point.get('weatherCondition', 'clear'),
            'average_temperature': weather_point.get('temperature') or weather_point.get('averageTemperature', 25),
            'humidity': weather_point.get('humidity', 60),
            'pressure': weather_point.get('pressure', 1013),
            'visibility_km': weather_point.get('visibility_km') or weather_point.get('visibilityKm', 10),
            'wind_speed_kmph': weather_point.get('wind_speed_kmph') or weather_point.get('windSpeedKmph', 10),
            'wind_direction': weather_point.get('wind_direction') or weather_point.get('windDirection', 'N'),
            'precipitation_mm': weather_point.get('precipitation') or weather_point.get('precipitationMm', 0),
            'road_surface_condition': weather_point.get('road_surface_condition') or weather_point.get('roadSurfaceCondition', 'dry'),
            'risk_score': weather_point.get('risk_score') or weather_point.get('riskScore', 3),
            'monsoon_risk': 8 if season == 'monsoon' else 3,
            'driving_condition_impact': weather_point.get('driving_condition_impact') or weather_point.get('drivingConditionImpact', 'minimal'),
            
            # Additional fields
            'cloud_cover': weather_point.get('cloud_cover', 50),
            'dewpoint': weather_point.get('dewpoint', 15),
            'uv_index': weather_point.get('uv_index', 5),
            'extreme_weather_history': weather_point.get('extreme_weather_history', []),
            'recommended_precautions': self._get_weather_precautions(season),
            
            # Metadata
            'data_source': 'ERA5_SEASONAL',
            'data_year': datetime.now().year,
            'forecast_accuracy': 85,
            'data_available': True
        }
        
        # Add camelCase versions for PDF compatibility
        weather_condition['weatherCondition'] = weather_condition['weather_condition']
        weather_condition['averageTemperature'] = weather_condition['average_temperature']
        weather_condition['visibilityKm'] = weather_condition['visibility_km']
        weather_condition['windSpeedKmph'] = weather_condition['wind_speed_kmph']
        weather_condition['windDirection'] = weather_condition['wind_direction']
        weather_condition['precipitationMm'] = weather_condition['precipitation_mm']
        weather_condition['roadSurfaceCondition'] = weather_condition['road_surface_condition']
        weather_condition['riskScore'] = weather_condition['risk_score']
        weather_condition['monsoonRisk'] = weather_condition['monsoon_risk']
        weather_condition['drivingConditionImpact'] = weather_condition['driving_condition_impact']
        
        # Store in database
        self.weather_condition_model.create_weather_condition(route_id, weather_condition)
        
        return weather_condition

    def _adjust_weather_for_season(self, base_weather: Dict, season: str) -> Dict:
        """Adjust weather data based on season"""
        adjusted = base_weather.copy()
        
        # Get base temperature
        base_temp = base_weather.get('temperature', 25)
        
        if season == 'summer':
            adjusted['temperature'] = base_temp + 10  # Hotter
            adjusted['humidity'] = max(30, base_weather.get('humidity', 60) - 20)
            adjusted['precipitation'] = 5
            adjusted['weather_condition'] = 'hot_and_dry'
            adjusted['risk_score'] = 5
            adjusted['visibility_km'] = 10
            adjusted['road_surface_condition'] = 'dry'
            
        elif season == 'monsoon':
            adjusted['temperature'] = base_temp - 2
            adjusted['humidity'] = min(95, base_weather.get('humidity', 60) + 25)
            adjusted['precipitation'] = 150
            adjusted['weather_condition'] = 'rainy'
            adjusted['risk_score'] = 8
            adjusted['visibility_km'] = 6
            adjusted['road_surface_condition'] = 'wet'
            adjusted['wind_speed_kmph'] = base_weather.get('wind_speed_kmph', 10) + 10
            
        elif season == 'autumn':
            adjusted['temperature'] = base_temp
            adjusted['humidity'] = base_weather.get('humidity', 60)
            adjusted['precipitation'] = 20
            adjusted['weather_condition'] = 'partly_cloudy'
            adjusted['risk_score'] = 3
            adjusted['visibility_km'] = 9
            adjusted['road_surface_condition'] = 'dry'
            
        elif season == 'winter':
            adjusted['temperature'] = base_temp - 7
            adjusted['humidity'] = base_weather.get('humidity', 60) - 5
            adjusted['precipitation'] = 0
            adjusted['weather_condition'] = 'foggy'
            adjusted['risk_score'] = 6
            adjusted['visibility_km'] = 5
            adjusted['road_surface_condition'] = 'dry'
        
        adjusted['season'] = season
        adjusted['monsoon_risk'] = 9 if season == 'monsoon' else 3
        adjusted['driving_condition_impact'] = 'significant' if season in ['monsoon', 'winter'] else 'minimal'
        
        return adjusted

    def _get_weather_precautions(self, season: str) -> List[str]:
        """Get recommended precautions based on season"""
        precautions_map = {
            'summer': [
                'Carry extra water',
                'Check coolant levels',
                'Use sun protection',
                'Avoid peak afternoon hours'
            ],
            'monsoon': [
                'Reduce speed significantly',
                'Maintain safe following distance',
                'Check wipers and tires',
                'Avoid waterlogged areas',
                'Use fog lights in heavy rain'
            ],
            'autumn': [
                'Watch for changing weather patterns',
                'Be prepared for occasional rain'
            ],
            'winter': [
                'Watch for morning fog',
                'Use fog lights when needed',
                'Drive slowly in low visibility'
            ],
            'spring': [
                'Standard safety precautions',
                'Check for seasonal weather changes'
            ]
        }
        return precautions_map.get(season, ['Standard safety precautions'])

    def _store_other_seasons(self, route_id: str, base_weather: Dict, api_data: Dict):
        """Store weather data for other seasons (optional)"""
        seasons = ['summer', 'monsoon', 'autumn', 'winter']
        current_season = base_weather['season']
        
        for season in seasons:
            if season != current_season:
                seasonal_weather = base_weather.copy()
                seasonal_weather['season'] = season
                
                # Adjust values based on season
                if season == 'summer':
                    seasonal_weather['averageTemperature'] = base_weather['averageTemperature'] + 8
                    seasonal_weather['humidity'] = max(30, base_weather['humidity'] - 20)
                    seasonal_weather['precipitationMm'] = 5
                    seasonal_weather['weatherCondition'] = 'hot_and_dry'
                    seasonal_weather['riskScore'] = 5
                elif season == 'monsoon':
                    seasonal_weather['averageTemperature'] = base_weather['averageTemperature'] - 3
                    seasonal_weather['humidity'] = min(95, base_weather['humidity'] + 25)
                    seasonal_weather['precipitationMm'] = 150
                    seasonal_weather['weatherCondition'] = 'rainy'
                    seasonal_weather['riskScore'] = 8
                    seasonal_weather['roadSurfaceCondition'] = 'wet'
                elif season == 'autumn':
                    seasonal_weather['precipitationMm'] = 20
                    seasonal_weather['weatherCondition'] = 'clear'
                    seasonal_weather['riskScore'] = 3
                elif season == 'winter':
                    seasonal_weather['averageTemperature'] = base_weather['averageTemperature'] - 5
                    seasonal_weather['humidity'] = base_weather['humidity'] - 10
                    seasonal_weather['precipitationMm'] = 0
                    seasonal_weather['weatherCondition'] = 'cloudy'
                    seasonal_weather['visibilityKm'] = 6
                    seasonal_weather['riskScore'] = 4
                
                seasonal_weather['monsoonRisk'] = 9 if season == 'monsoon' else 3
                seasonal_weather['recommendedPrecautions'] = self._get_weather_precautions(season)
                
                # Store seasonal variant
                self.weather_condition_model.create_weather_condition(route_id, seasonal_weather)
    
    def _get_era5_weather(self, point: Dict, coordinates: List[Dict], index: int, route_id: str) -> Optional[Dict]:
        """Get weather from single-point ERA5 API with better data handling"""
        try:
            params = {
                'latitude': point['latitude'],
                'longitude': point['longitude']
            }
            
            headers = {
                'X-API-Key': self.era5_api_key
            }
            
            logger.debug(f"ERA5 single-point API request: {self.era5_api_url} with params: {params}")
            start_time = time.time()
            response = requests.get(self.era5_api_url, params=params, headers=headers, timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            logger.debug(f"ERA5 API response: {response.status_code} in {response_time:.0f}ms")
            
            # Log API call
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='era5_weather',
                endpoint=self.era5_api_url,
                request_data=params,
                response_data={'status': 'success' if response.status_code == 200 else 'failed', 'status_code': response.status_code},
                status_code=response.status_code,
                response_time=response_time
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"ERA5 response data: {data}")
                
                # Check if data is available
                if data.get('data_available', False):
                    # Extract weather data
                    weather_data = {
                        'latitude': point['latitude'],
                        'longitude': point['longitude'],
                        'distance_from_start_km': self._calculate_cumulative_distance(coordinates, index),
                        'season': data.get('season', 'summer'),
                        'weather_condition': data.get('weather_condition', 'clear'),
                        'average_temperature': data.get('temperature', 25),
                        'humidity': data.get('humidity', 60),
                        'pressure': data.get('pressure', 1013),
                        'visibility_km': data.get('visibility_km', 10),
                        'wind_speed_kmph': data.get('wind_speed_kmph', 10),
                        'wind_direction': data.get('wind_direction', 'N'),
                        'precipitation_mm': data.get('precipitation', 0),
                        'road_surface_condition': data.get('road_surface_condition', 'dry'),
                        'risk_score': data.get('risk_score', 3),
                        'monsoon_risk': 8 if data.get('season') == 'monsoon' else 3,
                        'driving_condition_impact': data.get('driving_condition_impact', 'minimal'),
                        'data_source': 'ERA5_API',
                        'data_available': True
                    }
                    return weather_data
                else:
                    logger.warning(f"ERA5 API returned data_available=false for {point['latitude']}, {point['longitude']}")
                    return None
            else:
                logger.error(f"ERA5 API error: {response.status_code} - {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"ERA5 API timeout after 10 seconds")
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='era5_weather',
                endpoint=self.era5_api_url,
                request_data={'latitude': point['latitude'], 'longitude': point['longitude']},
                response_data=None,
                status_code=0,
                response_time=10000,
                error='Timeout'
            )
            return None
        except Exception as e:
            logger.error(f"Error fetching ERA5 weather: {e}")
            logger.error(traceback.format_exc())
            return None

    def _get_traffic_data_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Get traffic data with caching and batching"""
        logger.debug(f"Getting traffic data for route {route_id}")
        traffic_data = []
        
        try:
            # Sample fewer points
            sample_interval = max(1, len(coordinates) // 10)
            sample_points = coordinates[::sample_interval]
            logger.debug(f"Getting traffic for {len(sample_points)} sample points")
            
            for i, point in enumerate(sample_points):
                logger.debug(f"Processing traffic point {i+1}/{len(sample_points)}")
                
                # Check cache
                cached = self._get_cached_data('traffic', point['latitude'], point['longitude'])
                
                if cached:
                    traffic = cached
                    traffic['distance_from_start_km'] = self._calculate_cumulative_distance(
                        coordinates, i * sample_interval
                    )
                    logger.debug(f"Using CACHED traffic data for point {i+1}")
                else:
                    # Get from TomTom
                    logger.info(f"Calling TomTom API for traffic point {i+1}")
                    self._rate_limit_api('tomtom')
                    traffic = self._get_tomtom_traffic(point, coordinates, i * sample_interval, route_id)
                    
                    if traffic:
                        self._save_to_cache('traffic', point['latitude'], point['longitude'], traffic)
                
                if traffic:
                    self.traffic_data_model.create_traffic_data(route_id, traffic)
                    traffic_data.append(traffic)
            
            logger.debug(f"Traffic data complete: {len(traffic_data)} points")
            
        except Exception as e:
            logger.error(f"Error getting traffic data: {e}")
            logger.error(traceback.format_exc())
            
        return {'traffic_data': traffic_data}
    
    def _get_tomtom_traffic(self, point: Dict, coordinates: List[Dict], index: int, route_id: str) -> Optional[Dict]:
        """Get traffic from TomTom with proper error handling"""
        try:
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
            params = {
                'point': f"{point['latitude']},{point['longitude']}",
                'key': self.tomtom_api_key
            }
            
            logger.debug(f"TomTom API request: {url}")
            start_time = time.time()
            response = requests.get(url, params=params, timeout=5)
            response_time = (time.time() - start_time) * 1000
            
            logger.debug(f"TomTom API response: {response.status_code} in {response_time:.0f}ms")
            
            # Log API call
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='tomtom_traffic',
                endpoint=url,
                request_data=params,
                response_data={'status': 'success' if response.status_code == 200 else 'failed', 'status_code': response.status_code},
                status_code=response.status_code,
                response_time=response_time
            )
            
            if response.status_code == 200:
                data = response.json()
                flow_data = data.get('flowSegmentData', {})
                
                current_speed = flow_data.get('currentSpeed', 50)
                free_flow_speed = flow_data.get('freeFlowSpeed', 60)
                
                # Determine congestion level
                speed_ratio = current_speed / free_flow_speed if free_flow_speed > 0 else 1
                if speed_ratio >= 0.8:
                    congestion_level = 'free_flow'
                    risk_score = 2
                elif speed_ratio >= 0.6:
                    congestion_level = 'light'
                    risk_score = 3
                elif speed_ratio >= 0.4:
                    congestion_level = 'moderate'
                    risk_score = 5
                else:
                    congestion_level = 'heavy'
                    risk_score = 7
                
                return {
                    'latitude': point['latitude'],
                    'longitude': point['longitude'],
                    'distance_from_start_km': self._calculate_cumulative_distance(coordinates, index),
                    'average_speed_kmph': current_speed,
                    'free_flow_speed_kmph': free_flow_speed,
                    'congestion_level': congestion_level,
                    'confidence': flow_data.get('confidence', 0.8),
                    'road_closure': flow_data.get('roadClosure', False),
                    'risk_score': risk_score,
                    'peak_hour_traffic_count': 100 + (50 * (5 - risk_score)),
                    'time_of_day': 'current',
                    'data_source': 'TOMTOM_API'
                }
            else:
                logger.warning(f"TomTom API returned status code: {response.status_code}")
                # Return simulated traffic data
                return self._get_simulated_traffic(point, index, coordinates)
                
        except requests.exceptions.Timeout:
            logger.error(f"TomTom API timeout after 5 seconds")
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='tomtom_traffic',
                endpoint=url,
                request_data={'point': f"{point['latitude']},{point['longitude']}"},
                response_data=None,
                status_code=0,
                response_time=5000,
                error='Timeout'
            )
            return self._get_simulated_traffic(point, index, coordinates)
        except Exception as e:
            logger.error(f"Error fetching TomTom traffic: {e}")
            logger.error(traceback.format_exc())
            self.api_log_model.log_api_call(
                route_id=route_id,
                api_name='tomtom_traffic',
                endpoint=url,
                request_data={'point': f"{point['latitude']},{point['longitude']}"},
                response_data=None,
                status_code=0,
                response_time=0,
                error=str(e)
            )
            return self._get_simulated_traffic(point, index, coordinates)
    
    def _get_simulated_traffic(self, point: Dict, index: int, coordinates: List[Dict]) -> Dict:
        """Get simulated traffic data when API fails"""
        # Use risk calculator's traffic analysis
        traffic_data = self.risk_calculator.analyze_traffic_patterns(coordinates)
        if index < len(traffic_data):
            return traffic_data[index]
        
        # Default simulated data
        return {
            'latitude': point['latitude'],
            'longitude': point['longitude'],
            'distance_from_start_km': self._calculate_cumulative_distance(coordinates, index),
            'average_speed_kmph': 50,
            'free_flow_speed_kmph': 60,
            'congestion_level': 'light',
            'confidence': 0.5,
            'road_closure': False,
            'risk_score': 3,
            'peak_hour_traffic_count': 150,
            'time_of_day': 'current',
            'data_source': 'SIMULATED'
        }
    
    # Keep all the helper methods from the original
    def _calculate_total_distance(self, coordinates: List[Dict]) -> float:
        """Calculate total route distance"""
        total_distance = 0
        for i in range(1, len(coordinates)):
            distance = self.risk_calculator.calculate_distance(
                coordinates[i-1]['latitude'], coordinates[i-1]['longitude'],
                coordinates[i]['latitude'], coordinates[i]['longitude']
            )
            total_distance += distance
        return round(total_distance, 2)
    
    def _calculate_bounds(self, coordinates: List[Dict]) -> Dict:
        """Calculate bounding box for coordinates"""
        lats = [c['latitude'] for c in coordinates]
        lngs = [c['longitude'] for c in coordinates]
        
        return {
            'min_lat': min(lats) - 0.05,
            'max_lat': max(lats) + 0.05,
            'min_lng': min(lngs) - 0.05,
            'max_lng': max(lngs) + 0.05
        }
    
    def _calculate_distance_to_route(self, lat: float, lng: float, 
                                     route_points: List[Dict]) -> float:
        """Calculate minimum distance from a point to the route"""
        min_distance = float('inf')
        
        for point in route_points:
            distance = self.risk_calculator.calculate_distance(
                lat, lng, point['latitude'], point['longitude']
            )
            min_distance = min(min_distance, distance)
            
        return round(min_distance, 2)
    
    def _calculate_distance_along_route(self, lat: float, lng: float, 
                                       route_points: List[Dict]) -> float:
        """Calculate distance of a point along the route"""
        min_distance = float('inf')
        distance_along_route = 0
        
        for i in range(len(route_points) - 1):
            segment_distance = self.risk_calculator.calculate_distance(
                lat, lng, route_points[i]['latitude'], route_points[i]['longitude']
            )
            
            if segment_distance < min_distance:
                min_distance = segment_distance
                distance_along_route = self._calculate_cumulative_distance(route_points, i)
                
        return round(distance_along_route, 2)
    
    def _calculate_cumulative_distance(self, coordinates: List[Dict], up_to_index: int) -> float:
        """Calculate cumulative distance up to a specific point"""
        distance = 0
        for i in range(min(up_to_index, len(coordinates) - 1)):
            distance += self.risk_calculator.calculate_distance(
                coordinates[i]['latitude'], coordinates[i]['longitude'],
                coordinates[i+1]['latitude'], coordinates[i+1]['longitude']
            )
        return round(distance, 2)
    
    def _get_turn_severity(self, angle: float) -> str:
        """Determine turn severity based on angle"""
        if angle > 120:
            return 'hairpin'
        elif angle > 90:
            return 'sharp'
        elif angle > 60:
            return 'moderate'
        else:
            return 'gentle'
    
    def _enrich_emergency_service(self, service: Dict) -> Dict:
        """Add service-specific fields"""
        if service['service_type'] == 'hospital':
            service['services_offered'] = ['emergency', 'trauma', 'general']
            service['response_time'] = 10
            service['availability_score'] = 8
            service['priority'] = 'critical'
        elif service['service_type'] == 'police':
            service['services_offered'] = ['emergency_response', 'traffic_control']
            service['response_time'] = 15
            service['availability_score'] = 7
            service['priority'] = 'high'
        elif service['service_type'] == 'fire_station':
            service['services_offered'] = ['fire_response', 'rescue']
            service['response_time'] = 12
            service['availability_score'] = 7
            service['priority'] = 'high'
        elif service['service_type'] == 'fuel':
            service['service_type'] = 'mechanic'
            service['services_offered'] = ['fuel', 'repairs', 'rest']
            service['response_time'] = 0
            service['availability_score'] = 9
            service['priority'] = 'medium'
        elif service['service_type'] == 'school':
            service['service_type'] = 'educational'
            service['services_offered'] = ['education']
            service['response_time'] = 0
            service['availability_score'] = 5
            service['priority'] = 'low'
            
        return service
    
    def _enrich_accident_area(self, area: Dict) -> Dict:
        """Add accident area specific data"""
        area['accident_frequency_yearly'] = 5 + (area['risk_score'] * 2)
        area['time_of_day_risk'] = {
            'night': area['risk_score'],
            'day': max(1, area['risk_score'] - 2),
            'peak': area['risk_score'] - 1
        }
        area['weather_related_risk'] = min(10, area['risk_score'] + 1)
        area['infrastructure_risk'] = area.get('infrastructure_risk', 5)
        area['traffic_volume_risk'] = 5
        area['data_quality'] = 'high' if area['risk_score'] >= 8 else 'medium'
        
        return area
    
    def _identify_accident_prone_areas(self, sharp_turns: List[Dict], 
                                      road_conditions: List[Dict]) -> List[Dict]:
        """Identify accident-prone areas based on multiple factors"""
        accident_areas = []
        
        # Mark high-risk sharp turns as accident prone
        for turn in sharp_turns:
            if turn['risk_score'] >= 7:
                accident_areas.append({
                    'latitude': turn['latitude'],
                    'longitude': turn['longitude'],
                    'risk_score': turn['risk_score'],
                    'accident_type': 'sharp_turn',
                    'severity_level': 'high' if turn['risk_score'] >= 8 else 'medium',
                    'contributing_factors': [
                        f"{turn['turn_angle']}° turn",
                        turn['turn_direction'] + ' turn'
                    ],
                    'distance_from_start_km': turn['distance_from_start_km']
                })
        
        # Mark poor road conditions as accident prone
        for condition in road_conditions:
            if condition['risk_score'] >= 7:
                accident_areas.append({
                    'latitude': condition['latitude'],
                    'longitude': condition['longitude'],
                    'risk_score': condition['risk_score'],
                    'accident_type': 'poor_road_condition',
                    'severity_level': 'high' if condition['risk_score'] >= 8 else 'medium',
                    'contributing_factors': [
                        condition['surface_quality'] + ' surface',
                        condition['road_type']
                    ],
                    'distance_from_start_km': condition.get('distance_from_start_km', 0)
                })
                
        return accident_areas
    
    # Fallback to regular processing if fast mode fails
    def process_single_route_with_id(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Process with full API calls - fallback mode"""
        try:
            # Try fast mode first
            return self.process_single_route_fast(route_id, route_info, coordinates)
        except Exception as e:
            logger.error(f"Fast mode failed, error: {e}")
            raise e  # Re-raise the error for better debugging
    
    def reprocess_route(self, route_id: str):
        """Reprocess an existing route"""
        logger.info(f"Reprocessing route {route_id}")
        route = self.route_model.find_by_id(route_id)
        if not route:
            raise ValueError("Route not found")
            
        # Clear existing analysis data
        self._clear_route_analysis(route_id)
        
        # Reprocess
        coordinates = route.get('routePoints', [])
        if not coordinates:
            raise ValueError("No route points found")
            
        route_info = {
            'BU Code': route.get('fromCode', ''),
            'Row Labels': route.get('toCode', ''),
            'Customer Name': route.get('customerName', ''),
            'Location': route.get('location', '')
        }
        
        return self.process_single_route_fast(route_id, route_info, coordinates)
    
    def _clear_route_analysis(self, route_id: str):
        """Clear existing analysis data for a route"""
        logger.debug(f"Clearing analysis data for route {route_id}")
        route_obj_id = ObjectId(route_id)
        
        # Clear all related collections
        self.db.sharpturns.delete_many({'routeId': route_obj_id})
        self.db.blindspots.delete_many({'routeId': route_obj_id})
        self.db.accidentproneareas.delete_many({'routeId': route_obj_id})
        self.db.emergencyservices.delete_many({'routeId': route_obj_id})
        self.db.roadconditions.delete_many({'routeId': route_obj_id})
        self.db.networkcoverages.delete_many({'routeId': route_obj_id})
        self.db.ecosensitivezones.delete_many({'routeId': route_obj_id})
        self.db.weatherconditions.delete_many({'routeId': route_obj_id})
        self.db.trafficdata.delete_many({'routeId': route_obj_id})
    
    # Compatibility method
    def process_single_route_simulated(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Process a single route - redirects to fast processing"""
        return self.process_single_route_fast(route_id, route_info, coordinates)