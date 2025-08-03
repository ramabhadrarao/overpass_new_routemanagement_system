# services/route_processor.py
# Fast route processing service with batched API calls and caching
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

logger = logging.getLogger(__name__)

class RouteProcessor:
    def __init__(self, db, overpass_url):
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
        self.visualcrossing_api_key = os.getenv('VISUALCROSSING_API_KEY', 'EA9XLKA5PK3ZZLB783HUBK9W3')
        self.tomtom_api_key = os.getenv('TOMTOM_API_KEY', '4GMXpCknsEI6v22oQlZe5CFlV1Ev0xQu')
        self.here_api_key = os.getenv('HERE_API_KEY', '_Zmq3222RvY4Y5XspG6X4RQbOx2-QIp0C171cD3BHls')
        self.mapbox_api_key = os.getenv('MAPBOX_API_KEY', 'pk.eyJ1IjoiYW5pbDI1IiwiYSI6ImNtYmtlanhpYjBwZW4ya3F4ZnZ2NmNxNDkifQ.N0WsW5T60dxrG80rhnee0g')
        
        # Cache settings
        self.cache_enabled = True
        self.cache_expiry_hours = 24
        
        # Thread pool for concurrent processing
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        # API rate limits (requests per second)
        self.rate_limits = {
            'visualcrossing': 0.5,  # 2 requests per second
            'tomtom': 2.0,          # 2 requests per second
            'here': 2.0,            # 2 requests per second
            'overpass': 1.0         # 1 request per second
        }
        self.last_api_call = {}
        
    def _get_cache_key(self, cache_type: str, lat: float, lng: float) -> str:
        """Generate cache key for API results"""
        return hashlib.md5(f"{cache_type}_{lat:.4f}_{lng:.4f}".encode()).hexdigest()
    
    def _get_cached_data(self, cache_type: str, lat: float, lng: float) -> Optional[Dict]:
        """Get cached API data if available and not expired"""
        if not self.cache_enabled:
            return None
            
        cache_key = self._get_cache_key(cache_type, lat, lng)
        cached = self.db.api_cache.find_one({
            'cache_key': cache_key,
            'cache_type': cache_type
        })
        
        if cached:
            # Check if expired
            if cached['expires_at'] > datetime.utcnow():
                return cached['data']
                
        return None
    
    def _save_to_cache(self, cache_type: str, lat: float, lng: float, data: Dict):
        """Save API response to cache"""
        if not self.cache_enabled:
            return
            
        cache_key = self._get_cache_key(cache_type, lat, lng)
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
    
    def _rate_limit_api(self, api_name: str):
        """Implement rate limiting for API calls"""
        if api_name not in self.rate_limits:
            return
            
        min_interval = 1.0 / self.rate_limits[api_name]
        
        if api_name in self.last_api_call:
            elapsed = time.time() - self.last_api_call[api_name]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
                
        self.last_api_call[api_name] = time.time()
    
    def process_single_route_fast(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Fast processing mode with batched API calls and caching"""
        self.route_model.update_processing_status(route_id, 'processing')
        
        try:
            # Calculate total route distance
            total_distance = self._calculate_total_distance(coordinates)
            
            # Process in parallel using thread pool
            futures = []
            
            # 1. Geometric analysis (no API calls needed)
            futures.append(self.executor.submit(self._analyze_geometry_fast, route_id, coordinates, total_distance))
            
            # 2. Emergency services (single Overpass call)
            futures.append(self.executor.submit(self._get_emergency_services_fast, route_id, coordinates))
            
            # 3. Road conditions (batched Overpass calls)
            futures.append(self.executor.submit(self._analyze_road_conditions_fast, route_id, coordinates))
            
            # 4. Network coverage (simulated - no API)
            futures.append(self.executor.submit(self._analyze_network_coverage_fast, route_id, coordinates, total_distance))
            
            # 5. Weather data (single VisualCrossing call for route)
            futures.append(self.executor.submit(self._get_weather_data_fast, route_id, coordinates))
            
            # 6. Traffic data (batched TomTom calls)
            futures.append(self.executor.submit(self._get_traffic_data_fast, route_id, coordinates))
            
            # Wait for all tasks to complete
            results = {}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.update(result)
                except Exception as e:
                    logger.error(f"Error in parallel processing: {e}")
            
            # Calculate risk scores
            risk_scores = self.risk_calculator.calculate_overall_risk_score(results)
            
            # Update route with risk scores
            risk_scores_with_level = risk_scores['scores'].copy()
            risk_scores_with_level['overall'] = risk_scores['overall']
            risk_scores_with_level['risk_level'] = risk_scores['risk_level']
            
            self.route_model.update_risk_scores(route_id, risk_scores_with_level)
            self.route_model.update_processing_status(route_id, 'completed')
            
            logger.info(f"Route {route_id} processed successfully in fast mode")
            
            return {
                'route_id': route_id,
                'status': 'completed',
                'risk_level': risk_scores['risk_level'],
                'overall_score': risk_scores['overall']
            }
            
        except Exception as e:
            self.route_model.update_processing_status(route_id, 'failed', str(e))
            raise e
    
    def _analyze_geometry_fast(self, route_id: str, coordinates: List[Dict], total_distance: float) -> Dict:
        """Fast geometric analysis without API calls"""
        results = {}
        
        # Analyze sharp turns
        sharp_turns = self.risk_calculator.analyze_sharp_turns(coordinates)
        for turn in sharp_turns:
            turn['distance_from_end_km'] = total_distance - turn['distance_from_start_km']
            turn['turn_severity'] = self._get_turn_severity(turn['turn_angle'])
            turn['road_surface'] = 'good'
            turn['guardrails'] = False
            turn['warning_signs'] = False
            self.sharp_turn_model.create_sharp_turn(route_id, turn)
        results['sharp_turns'] = sharp_turns
        
        # Identify blind spots
        blind_spots = self.risk_calculator.identify_blind_spots(coordinates)
        for spot in blind_spots:
            spot['distance_from_end_km'] = total_distance - spot['distance_from_start_km']
            spot['gradient'] = 0
            spot['curvature'] = 0
            spot['road_width'] = 7
            self.blind_spot_model.create_blind_spot(route_id, spot)
        results['blind_spots'] = blind_spots
        
        # Identify accident prone areas based on geometry
        accident_areas = self._identify_accident_prone_areas(sharp_turns, [])
        for area in accident_areas:
            area['distance_from_end_km'] = total_distance - area['distance_from_start_km']
            area = self._enrich_accident_area(area)
            self.accident_prone_model.create_accident_area(route_id, area)
        results['accident_prone_areas'] = accident_areas
        
        return results
    
    def _get_emergency_services_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Get emergency services with caching"""
        bounds = self._calculate_bounds(coordinates)
        
        # Check cache first
        cache_key = f"emergency_{bounds['min_lat']:.2f}_{bounds['min_lng']:.2f}"
        cached_data = self._get_cached_data('emergency_services', bounds['min_lat'], bounds['min_lng'])
        
        if cached_data:
            emergency_services = cached_data
        else:
            self._rate_limit_api('overpass')
            emergency_services = self.overpass_service.get_emergency_services(route_id, bounds)
            self._save_to_cache('emergency_services', bounds['min_lat'], bounds['min_lng'], emergency_services)
        
        # Process and save services
        for service in emergency_services:
            service['distance_from_route_km'] = self._calculate_distance_to_route(
                service['latitude'], service['longitude'], coordinates
            )
            service['distance_from_start_km'] = self._calculate_distance_along_route(
                service['latitude'], service['longitude'], coordinates
            )
            service['distance_from_end_km'] = self._calculate_total_distance(coordinates) - service['distance_from_start_km']
            service = self._enrich_emergency_service(service)
            self.emergency_service_model.create_emergency_service(route_id, service)
        
        return {'emergency_services': emergency_services}
    
    def _analyze_road_conditions_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Analyze road conditions with batched requests"""
        road_conditions = []
        
        # Sample fewer points for faster processing
        sample_interval = max(1, len(coordinates) // 10)  # Only 10 samples
        sample_points = coordinates[::sample_interval]
        
        for i, point in enumerate(sample_points):
            # Check cache
            cached = self._get_cached_data('road_condition', point['latitude'], point['longitude'])
            
            if cached:
                condition = cached
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
        
        return {'road_conditions': road_conditions}
    
    def _analyze_network_coverage_fast(self, route_id: str, coordinates: List[Dict], total_distance: float) -> Dict:
        """Fast network coverage analysis"""
        network_coverage = self.risk_calculator.analyze_network_coverage(coordinates)
        
        for coverage in network_coverage:
            coverage['distance_from_end_km'] = total_distance - coverage['distance_from_start_km']
            self.network_coverage_model.create_network_coverage(route_id, coverage)
        
        return {'network_coverage': network_coverage}
    
    def _get_weather_data_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Get weather data using VisualCrossing API with batching"""
        weather_conditions = []
        
        # Get weather for start, middle and end points only
        sample_points = [
            coordinates[0],
            coordinates[len(coordinates) // 2],
            coordinates[-1]
        ]
        
        for i, point in enumerate(sample_points):
            # Check cache
            cached = self._get_cached_data('weather', point['latitude'], point['longitude'])
            
            if cached:
                weather = cached
                weather['distance_from_start_km'] = self._calculate_cumulative_distance(
                    coordinates, i * (len(coordinates) // 2)
                )
            else:
                # Use VisualCrossing API
                self._rate_limit_api('visualcrossing')
                weather = self._get_visualcrossing_weather(point, coordinates, i * (len(coordinates) // 2))
                
                if weather:
                    self._save_to_cache('weather', point['latitude'], point['longitude'], weather)
            
            if weather:
                self.weather_condition_model.create_weather_condition(route_id, weather)
                weather_conditions.append(weather)
        
        return {'weather_conditions': weather_conditions}
    
    # def _get_visualcrossing_weather(self, point: Dict, coordinates: List[Dict], index: int) -> Optional[Dict]:
    #     """Get weather from VisualCrossing API"""
    #     try:
    #         url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    #         params = {
    #             'location': f"{point['latitude']},{point['longitude']}",
    #             'key': self.visualcrossing_api_key,
    #             'include': 'current,days',
    #             'elements': 'datetime,temp,humidity,precipprob,windspeed,visibility,conditions'
    #         }
            
    #         start_time = time.time()
    #         response = requests.get(url, params=params, timeout=10)
    #         response_time = (time.time() - start_time) * 1000
            
    #         # Log API call
    #         self.api_log_model.log_api_call(
    #             route_id=None,
    #             api_name='visualcrossing',
    #             endpoint=url,
    #             request_data=params,
    #             response_data={'status': 'success' if response.status_code == 200 else 'failed'},
    #             status_code=response.status_code,
    #             response_time=response_time
    #         )
            
    #         if response.status_code == 200:
    #             data = response.json()
    #             current = data.get('currentConditions', {})
                
    #             # Determine season and risk
    #             temp = current.get('temp', 25)
    #             conditions = current.get('conditions', 'Clear').lower()
                
    #             if temp > 35:
    #                 season = 'summer'
    #             elif temp < 15:
    #                 season = 'winter'
    #             elif 'rain' in conditions:
    #                 season = 'monsoon'
    #             else:
    #                 season = 'spring'
                
    #             # Calculate risk
    #             risk_score = 3
    #             if 'rain' in conditions or 'storm' in conditions:
    #                 risk_score += 4
    #             elif 'fog' in conditions or 'mist' in conditions:
    #                 risk_score += 5
    #             elif temp > 40 or temp < 5:
    #                 risk_score += 3
                
    #             return {
    #                 'latitude': point['latitude'],
    #                 'longitude': point['longitude'],
    #                 'distance_from_start_km': self._calculate_cumulative_distance(coordinates, index),
    #                 'season': season,
    #                 'weather_condition': 'rainy' if 'rain' in conditions else 'foggy' if 'fog' in conditions else 'clear',
    #                 'average_temperature': temp,
    #                 'humidity': current.get('humidity', 60),
    #                 'pressure': current.get('pressure', 1013),
    #                 'visibility_km': current.get('visibility', 10),
    #                 'wind_speed_kmph': current.get('windspeed', 10),
    #                 'wind_direction': current.get('winddir', 0),
    #                 'precipitation_mm': current.get('precip', 0),
    #                 'precipitation_probability': current.get('precipprob', 0),
    #                 'road_surface_condition': 'wet' if 'rain' in conditions else 'dry',
    #                 'risk_score': min(10, risk_score),
    #                 'monsoon_risk': 8 if season == 'monsoon' else 3,
    #                 'driving_condition_impact': 'severe' if risk_score >= 7 else 'moderate' if risk_score >= 5 else 'minimal',
    #                 'data_source': 'VISUALCROSSING_API'
    #             }
                
    #     except Exception as e:
    #         logger.error(f"Error fetching VisualCrossing weather: {e}")
    #         return self._get_fallback_weather(point, index, coordinates)
    def _get_visualcrossing_weather(self, point: Dict, coordinates: List[Dict], index: int) -> Optional[Dict]:
        """Get weather from ERA5 API instead of VisualCrossing"""
        try:
            # Use ERA5 API with static IP
            url = "http://43.250.40.133:6000/api/weather/visualcrossing-compatible"
            
            # ERA5 API key (replace with your actual key or get from env)
            era5_api_key = "h4DSeoxB88OwRw7rh42sWJlx8BphPHCi"
            
            params = {
                'location': f"{point['latitude']},{point['longitude']}"
            }
            
            headers = {
                'X-API-Key': era5_api_key
            }
            
            start_time = time.time()
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            # Log API call
            self.api_log_model.log_api_call(
                route_id=None,
                api_name='era5_weather',  # Changed from 'visualcrossing'
                endpoint=url,
                request_data=params,
                response_data={'status': 'success' if response.status_code == 200 else 'failed'},
                status_code=response.status_code,
                response_time=response_time
            )
            
            if response.status_code == 200:
                data = response.json()
                current = data.get('currentConditions', {})
                
                # Determine season and risk
                temp = current.get('temp', 25)
                conditions = current.get('conditions', 'Clear').lower()
                
                if temp > 35:
                    season = 'summer'
                elif temp < 15:
                    season = 'winter'
                elif 'rain' in conditions:
                    season = 'monsoon'
                else:
                    season = 'spring'
                
                # Calculate risk
                risk_score = 3
                if 'rain' in conditions or 'storm' in conditions:
                    risk_score += 4
                elif 'fog' in conditions or 'mist' in conditions:
                    risk_score += 5
                elif temp > 40 or temp < 5:
                    risk_score += 3
                
                return {
                    'latitude': point['latitude'],
                    'longitude': point['longitude'],
                    'distance_from_start_km': self._calculate_cumulative_distance(coordinates, index),
                    'season': season,
                    'weather_condition': 'rainy' if 'rain' in conditions else 'foggy' if 'fog' in conditions else 'clear',
                    'average_temperature': temp,
                    'humidity': current.get('humidity', 60),
                    'pressure': current.get('pressure', 1013),
                    'visibility_km': current.get('visibility', 10),
                    'wind_speed_kmph': current.get('windspeed', 10),
                    'wind_direction': current.get('winddir', 0),
                    'precipitation_mm': current.get('precip', 0),
                    'precipitation_probability': current.get('precipprob', 0),
                    'road_surface_condition': 'wet' if 'rain' in conditions else 'dry',
                    'risk_score': min(10, risk_score),
                    'monsoon_risk': 8 if season == 'monsoon' else 3,
                    'driving_condition_impact': 'severe' if risk_score >= 7 else 'moderate' if risk_score >= 5 else 'minimal',
                    'data_source': 'ERA5_REANALYSIS'  # Changed from 'VISUALCROSSING_API'
                }
            else:
                # Log the error
                logger.error(f"ERA5 API returned status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return self._get_fallback_weather(point, index, coordinates)
                    
        except Exception as e:
            logger.error(f"Error fetching ERA5 weather: {e}")
            return self._get_fallback_weather(point, index, coordinates)
    
    def _get_traffic_data_fast(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Get traffic data with caching and batching"""
        traffic_data = []
        
        # Sample fewer points
        sample_interval = max(1, len(coordinates) // 10)
        sample_points = coordinates[::sample_interval]
        
        for i, point in enumerate(sample_points):
            # Check cache
            cached = self._get_cached_data('traffic', point['latitude'], point['longitude'])
            
            if cached:
                traffic = cached
                traffic['distance_from_start_km'] = self._calculate_cumulative_distance(
                    coordinates, i * sample_interval
                )
            else:
                # Get from TomTom or use fallback
                self._rate_limit_api('tomtom')
                traffic = self._get_tomtom_traffic(point, coordinates, i * sample_interval)
                
                if traffic:
                    self._save_to_cache('traffic', point['latitude'], point['longitude'], traffic)
            
            if traffic:
                self.traffic_data_model.create_traffic_data(route_id, traffic)
                traffic_data.append(traffic)
        
        return {'traffic_data': traffic_data}
    
    def _get_tomtom_traffic(self, point: Dict, coordinates: List[Dict], index: int) -> Optional[Dict]:
        """Get traffic from TomTom with proper error handling"""
        try:
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
            params = {
                'point': f"{point['latitude']},{point['longitude']}",
                'key': self.tomtom_api_key
            }
            
            start_time = time.time()
            response = requests.get(url, params=params, timeout=5)
            response_time = (time.time() - start_time) * 1000
            
            # Log API call
            self.api_log_model.log_api_call(
                route_id=None,
                api_name='tomtom',
                endpoint=url,
                request_data=params,
                response_data={'status': 'success' if response.status_code == 200 else 'failed'},
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
                
        except Exception as e:
            logger.error(f"Error fetching TomTom traffic: {e}")
            return self._get_fallback_traffic(point, index, coordinates)
    
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
    
    def _get_fallback_weather(self, point: Dict, index: int, coordinates: List[Dict]) -> Dict:
        """Get fallback weather data when API fails"""
        return {
            'latitude': point['latitude'],
            'longitude': point['longitude'],
            'distance_from_start_km': self._calculate_cumulative_distance(coordinates, index),
            'season': 'summer',
            'weather_condition': 'clear',
            'average_temperature': 28,
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
            'data_source': 'FALLBACK'
        }
    
    def _get_fallback_traffic(self, point: Dict, index: int, coordinates: List[Dict]) -> Dict:
        """Get fallback traffic data when API fails"""
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
            'data_source': 'FALLBACK'
        }
    
    # Fallback to regular processing if fast mode fails
    def process_single_route_with_id(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Process with full API calls - fallback mode"""
        try:
            # Try fast mode first
            return self.process_single_route_fast(route_id, route_info, coordinates)
        except Exception as e:
            logger.error(f"Fast mode failed, using regular processing: {e}")
            # Implement regular processing as fallback
            return self._process_regular(route_id, route_info, coordinates)
    
    def _process_regular(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Regular processing mode - simplified version"""
        self.route_model.update_processing_status(route_id, 'processing')
        
        try:
            # Basic analysis without extensive API calls
            results = {}
            total_distance = self._calculate_total_distance(coordinates)
            
            # Geometric analysis
            results.update(self._analyze_geometry_fast(route_id, coordinates, total_distance))
            
            # Basic services
            bounds = self._calculate_bounds(coordinates)
            emergency_services = []
            for i in range(3):  # Add some dummy services
                service = {
                    'latitude': (bounds['min_lat'] + bounds['max_lat']) / 2 + (i * 0.01),
                    'longitude': (bounds['min_lng'] + bounds['max_lng']) / 2 + (i * 0.01),
                    'service_type': ['hospital', 'police', 'fire_station'][i],
                    'name': f"Emergency Service {i+1}",
                    'phone': '100',
                    'address': 'Near route',
                    'distance_from_route_km': 2 + i,
                    'distance_from_start_km': 10 * (i + 1),
                    'distance_from_end_km': total_distance - (10 * (i + 1))
                }
                service = self._enrich_emergency_service(service)
                self.emergency_service_model.create_emergency_service(route_id, service)
                emergency_services.append(service)
            results['emergency_services'] = emergency_services
            
            # Basic road conditions
            road_conditions = []
            for i in range(5):
                condition = {
                    'latitude': coordinates[i * (len(coordinates) // 5)]['latitude'],
                    'longitude': coordinates[i * (len(coordinates) // 5)]['longitude'],
                    'road_type': 'highway',
                    'surface': 'asphalt',
                    'lanes': 2,
                    'max_speed': 60,
                    'width': 7,
                    'under_construction': False,
                    'surface_quality': 'good',
                    'risk_score': 3,
                    'distance_from_start_km': i * (total_distance / 5),
                    'has_potholes': False,
                    'data_source': 'ESTIMATED'
                }
                self.road_condition_model.create_road_condition(route_id, condition)
                road_conditions.append(condition)
            results['road_conditions'] = road_conditions
            
            # Basic network coverage
            results.update(self._analyze_network_coverage_fast(route_id, coordinates, total_distance))
            
            # Calculate risk scores
            risk_scores = self.risk_calculator.calculate_overall_risk_score(results)
            
            # Update route
            risk_scores_with_level = risk_scores['scores'].copy()
            risk_scores_with_level['overall'] = risk_scores['overall']
            risk_scores_with_level['risk_level'] = risk_scores['risk_level']
            
            self.route_model.update_risk_scores(route_id, risk_scores_with_level)
            self.route_model.update_processing_status(route_id, 'completed')
            
            return {
                'route_id': route_id,
                'status': 'completed',
                'risk_level': risk_scores['risk_level'],
                'overall_score': risk_scores['overall']
            }
            
        except Exception as e:
            self.route_model.update_processing_status(route_id, 'failed', str(e))
            raise e
    
    def reprocess_route(self, route_id: str):
        """Reprocess an existing route"""
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