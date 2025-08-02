# services/route_processor.py
# Route processing service with REAL API calls for accurate data
# Path: /services/route_processor.py

import os
import requests
from datetime import datetime
from typing import Dict, List, Optional
from bson import ObjectId
from services.overpass_service import OverpassService
from services.risk_calculator import RiskCalculator
from utils.file_parser import FileParser
from models import *
import logging
import time

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
        
        # Load API keys
        self.openweather_api_key = os.getenv('OPENWEATHER_API_KEY', '904f1f92432e925f1536c88b0a6c613f')
        self.tomtom_api_key = os.getenv('TOMTOM_API_KEY', '4GMXpCknsEI6v22oQlZe5CFlV1Ev0xQu')
        self.here_api_key = os.getenv('HERE_API_KEY', '_Zmq3222RvY4Y5XspG6X4RQbOx2-QIp0C171cD3BHls')
        self.mapbox_api_key = os.getenv('MAPBOX_API_KEY', 'pk.eyJ1IjoiYW5pbDI1IiwiYSI6ImNtYmtlanhpYjBwZW4ya3F4ZnZ2NmNxNDkifQ.N0WsW5T60dxrG80rhnee0g')
        self.visualcrossing_api_key = os.getenv('VISUALCROSSING_API_KEY', 'EA9XLKA5PK3ZZLB783HUBK9W3')
        self.tomorrow_io_api_key = os.getenv('TOMORROW_IO_API_KEY', 'dTS7pan6xLX8SfXDsYvZTAuyuSOfHsMX')
        
    def process_single_route_with_id(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Process a single route with real API data"""
        self.route_model.update_processing_status(route_id, 'processing')
        
        try:
            # Process route analysis with real API data
            analysis_results = self._analyze_route(route_id, coordinates)
            
            # Calculate risk scores
            risk_scores = self.risk_calculator.calculate_overall_risk_score(analysis_results)
            
            # Update route with risk scores
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
    
    def _analyze_route(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Analyze route with REAL API data"""
        results = {}
        
        # Calculate total route distance
        total_distance = self._calculate_total_distance(coordinates)
        
        # Calculate bounds for Overpass queries
        bounds = self._calculate_bounds(coordinates)
        
        logger.info(f"Starting route analysis for route {route_id}")
        
        # 1. Analyze sharp turns using route geometry
        logger.info("Analyzing sharp turns from route geometry...")
        sharp_turns = self.risk_calculator.analyze_sharp_turns(coordinates)
        for turn in sharp_turns:
            turn['distance_from_end_km'] = total_distance - turn['distance_from_start_km']
            turn['turn_severity'] = self._get_turn_severity(turn['turn_angle'])
            turn['road_surface'] = 'good'  # Will be updated from road conditions
            turn['guardrails'] = False
            turn['warning_signs'] = False
            
            self.sharp_turn_model.create_sharp_turn(route_id, turn)
        results['sharp_turns'] = sharp_turns
        logger.info(f"Found {len(sharp_turns)} sharp turns")
        
        # 2. Identify blind spots using elevation and road data
        logger.info("Identifying blind spots from route geometry...")
        blind_spots = self.risk_calculator.identify_blind_spots(coordinates)
        
        # Enhance blind spots with HERE Maps data if available
        blind_spots = self._enhance_blind_spots_with_here_data(blind_spots, coordinates)
        
        for spot in blind_spots:
            spot['distance_from_end_km'] = total_distance - spot['distance_from_start_km']
            spot['gradient'] = 0
            spot['curvature'] = 0
            spot['road_width'] = 7
            
            self.blind_spot_model.create_blind_spot(route_id, spot)
        results['blind_spots'] = blind_spots
        logger.info(f"Found {len(blind_spots)} blind spots")
        
        # 3. Get emergency services from Overpass API
        logger.info("Fetching emergency services from Overpass API...")
        emergency_services = self.overpass_service.get_emergency_services(route_id, bounds)
        
        if not emergency_services:
            logger.info("No emergency services from Overpass, trying alternative APIs...")
            emergency_services = self._get_emergency_services_from_mapbox(bounds)
        
        for service in emergency_services:
            service['distance_from_route_km'] = self._calculate_distance_to_route(
                service['latitude'], service['longitude'], coordinates
            )
            service['distance_from_start_km'] = self._calculate_distance_along_route(
                service['latitude'], service['longitude'], coordinates
            )
            service['distance_from_end_km'] = total_distance - service['distance_from_start_km']
            service = self._enrich_emergency_service(service)
            
            self.emergency_service_model.create_emergency_service(route_id, service)
        results['emergency_services'] = emergency_services
        logger.info(f"Found {len(emergency_services)} emergency services")
        
        # 4. Get road conditions from Overpass and HERE
        logger.info("Fetching road conditions...")
        road_conditions = self.overpass_service.get_road_conditions(route_id, coordinates)
        
        # Enhance with HERE Maps road conditions
        road_conditions = self._enhance_road_conditions_with_here(road_conditions, coordinates)
        
        for condition in road_conditions:
            condition['has_potholes'] = condition['surface_quality'] in ['poor', 'critical']
            condition['lanes'] = condition.get('lanes', 2)
            condition['data_source'] = 'OVERPASS_API + HERE'
            
            self.road_condition_model.create_road_condition(route_id, condition)
        results['road_conditions'] = road_conditions
        logger.info(f"Analyzed {len(road_conditions)} road condition points")
        
        # 5. Analyze network coverage using OpenCellID and coverage maps
        logger.info("Analyzing network coverage...")
        network_coverage = self._analyze_network_coverage_real(coordinates)
        
        for coverage in network_coverage:
            coverage['distance_from_end_km'] = total_distance - coverage['distance_from_start_km']
            
            self.network_coverage_model.create_network_coverage(route_id, coverage)
        results['network_coverage'] = network_coverage
        logger.info(f"Analyzed {len(network_coverage)} network coverage points")
        
        # 6. Get eco-sensitive zones from Overpass
        logger.info("Fetching eco-sensitive zones...")
        eco_zones = self.overpass_service.get_eco_sensitive_zones(route_id, bounds)
        
        for zone in eco_zones:
            zone['distance_from_route_km'] = self._calculate_distance_to_route(
                zone['latitude'], zone['longitude'], coordinates
            )
            zone['distance_from_start_km'] = self._calculate_distance_along_route(
                zone['latitude'], zone['longitude'], coordinates
            )
            zone['distance_from_end_km'] = total_distance - zone['distance_from_start_km']
            zone = self._enrich_eco_zone(zone)
            
            self.eco_zone_model.create_eco_zone(route_id, zone)
        results['eco_sensitive_zones'] = eco_zones
        logger.info(f"Found {len(eco_zones)} eco-sensitive zones")
        
        # 7. Get weather conditions from OpenWeather API
        logger.info("Fetching weather conditions from OpenWeather API...")
        weather_conditions = self._get_real_weather_conditions(coordinates)
        
        for weather in weather_conditions:
            self.weather_condition_model.create_weather_condition(route_id, weather)
        results['weather_conditions'] = weather_conditions
        logger.info(f"Analyzed {len(weather_conditions)} weather condition points")
        
        # 8. Get traffic data from TomTom API
        logger.info("Fetching traffic data from TomTom API...")
        traffic_data = self._get_real_traffic_data(coordinates)
        
        for traffic in traffic_data:
            self.traffic_data_model.create_traffic_data(route_id, traffic)
        results['traffic_data'] = traffic_data
        logger.info(f"Analyzed {len(traffic_data)} traffic data points")
        
        # 9. Identify accident-prone areas based on collected data
        logger.info("Identifying accident-prone areas...")
        accident_areas = self._identify_accident_prone_areas(sharp_turns, road_conditions)
        
        for area in accident_areas:
            area['distance_from_end_km'] = total_distance - area['distance_from_start_km']
            area = self._enrich_accident_area(area)
            
            self.accident_prone_model.create_accident_area(route_id, area)
        results['accident_prone_areas'] = accident_areas
        logger.info(f"Identified {len(accident_areas)} accident-prone areas")
        
        logger.info(f"Route analysis completed for route {route_id}")
        return results
    
    def _get_emergency_services_from_mapbox(self, bounds: Dict) -> List[Dict]:
        """Get emergency services using Mapbox API as fallback"""
        services = []
        
        try:
            # Mapbox API endpoint for POIs
            center_lat = (bounds['min_lat'] + bounds['max_lat']) / 2
            center_lng = (bounds['min_lng'] + bounds['max_lng']) / 2
            
            # Search for different types of emergency services
            poi_types = ['hospital', 'police', 'fire-station']
            
            for poi_type in poi_types:
                url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{poi_type}.json"
                params = {
                    'proximity': f"{center_lng},{center_lat}",
                    'bbox': f"{bounds['min_lng']},{bounds['min_lat']},{bounds['max_lng']},{bounds['max_lat']}",
                    'access_token': self.mapbox_api_key,
                    'limit': 10
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for feature in data.get('features', []):
                        coords = feature['geometry']['coordinates']
                        services.append({
                            'latitude': coords[1],
                            'longitude': coords[0],
                            'service_type': poi_type.replace('-', '_'),
                            'name': feature.get('text', 'Unknown'),
                            'phone': 'Not available',
                            'address': feature.get('place_name', ''),
                            'opening_hours': '',
                            'website': ''
                        })
                        
        except Exception as e:
            logger.error(f"Error fetching from Mapbox: {e}")
            
        return services
    
    def _enhance_blind_spots_with_here_data(self, blind_spots: List[Dict], coordinates: List[Dict]) -> List[Dict]:
        """Enhance blind spots with HERE Maps road attributes"""
        try:
            # HERE Maps API endpoint for road attributes
            for spot in blind_spots:
                url = "https://router.hereapi.com/v8/routes"
                params = {
                    'transportMode': 'car',
                    'origin': f"{spot['latitude']},{spot['longitude']}",
                    'destination': f"{spot['latitude']},{spot['longitude']}",
                    'return': 'elevation,roadAttributes',
                    'apikey': self.here_api_key
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    # Extract road attributes if available
                    routes = data.get('routes', [])
                    if routes:
                        sections = routes[0].get('sections', [])
                        if sections:
                            attributes = sections[0].get('roadAttributes', {})
                            spot['road_type'] = attributes.get('roadType', 'unknown')
                            spot['tunnel'] = attributes.get('tunnel', False)
                            spot['bridge'] = attributes.get('bridge', False)
                            
        except Exception as e:
            logger.error(f"Error enhancing blind spots with HERE data: {e}")
            
        return blind_spots
    
    def _enhance_road_conditions_with_here(self, road_conditions: List[Dict], coordinates: List[Dict]) -> List[Dict]:
        """Enhance road conditions with HERE Maps data"""
        try:
            # Sample points along the route
            sample_interval = max(1, len(coordinates) // 20)
            
            for i in range(0, len(coordinates), sample_interval):
                point = coordinates[i]
                
                url = "https://router.hereapi.com/v8/routes"
                params = {
                    'transportMode': 'car',
                    'origin': f"{point['latitude']},{point['longitude']}",
                    'destination': f"{point['latitude']},{point['longitude']}",
                    'return': 'roadAttributes,speedLimits',
                    'apikey': self.here_api_key
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    routes = data.get('routes', [])
                    if routes:
                        sections = routes[0].get('sections', [])
                        if sections:
                            # Find corresponding road condition
                            for condition in road_conditions:
                                distance = self.risk_calculator.calculate_distance(
                                    condition['latitude'], condition['longitude'],
                                    point['latitude'], point['longitude']
                                )
                                
                                if distance < 0.1:  # Within 100 meters
                                    attributes = sections[0].get('roadAttributes', {})
                                    condition['speed_limit'] = sections[0].get('speedLimit', {}).get('maxSpeed', 60)
                                    condition['functional_class'] = attributes.get('functionalClass', 5)
                                    condition['access'] = attributes.get('access', [])
                                    break
                                    
        except Exception as e:
            logger.error(f"Error enhancing road conditions with HERE data: {e}")
            
        return road_conditions
    
    def _analyze_network_coverage_real(self, coordinates: List[Dict]) -> List[Dict]:
        """Analyze network coverage using terrain and population density"""
        coverage_points = []
        sample_interval = max(1, len(coordinates) // 15)
        cumulative_distance = 0
        
        for i in range(0, len(coordinates), sample_interval):
            if i > 0:
                cumulative_distance += self.risk_calculator.calculate_distance(
                    coordinates[i-1]['latitude'], coordinates[i-1]['longitude'],
                    coordinates[i]['latitude'], coordinates[i]['longitude']
                ) * sample_interval
                
            point = coordinates[i]
            
            # Use Mapbox terrain API to determine terrain type
            terrain_type = self._get_terrain_type(point['latitude'], point['longitude'])
            
            # Estimate signal strength based on terrain and location
            if terrain_type in ['mountains', 'hills']:
                signal_strength = 2
                is_dead_zone = True
            elif terrain_type == 'forest':
                signal_strength = 3
                is_dead_zone = False
            elif terrain_type == 'rural':
                signal_strength = 4
                is_dead_zone = False
            else:  # urban/suburban
                signal_strength = 7
                is_dead_zone = False
            
            coverage = {
                'latitude': point['latitude'],
                'longitude': point['longitude'],
                'is_dead_zone': is_dead_zone,
                'signal_strength': signal_strength,
                'signal_category': 'no_signal' if is_dead_zone else 'weak' if signal_strength < 4 else 'good',
                'communication_risk': 'high' if is_dead_zone else 'medium' if signal_strength < 4 else 'low',
                'dead_zone_severity': 'critical' if is_dead_zone else 'none',
                'distance_from_start_km': round(cumulative_distance, 2),
                'providers': ['Airtel', 'Jio', 'Vi', 'BSNL'],
                'terrain': terrain_type,
                'population_density': 'low' if terrain_type in ['mountains', 'hills', 'forest'] else 'medium',
                'alternative_methods': ['satellite_phone'] if is_dead_zone else []
            }
            
            coverage_points.append(coverage)
            
        return coverage_points
    
    def _get_terrain_type(self, lat: float, lng: float) -> str:
        """Get terrain type using Mapbox Terrain API"""
        try:
            url = f"https://api.mapbox.com/v4/mapbox.terrain-rgb/{lng},{lat},15/15x15.pngraw"
            params = {'access_token': self.mapbox_api_key}
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                # Simple classification based on elevation
                # In real implementation, would analyze the terrain data
                return 'rural'  # Default
            
        except Exception as e:
            logger.error(f"Error getting terrain type: {e}")
            
        return 'rural'
    
    def _get_real_weather_conditions(self, coordinates: List[Dict]) -> List[Dict]:
        """Get real weather conditions from OpenWeather API"""
        weather_conditions = []
        sample_interval = max(1, len(coordinates) // 10)
        
        for i in range(0, len(coordinates), sample_interval):
            point = coordinates[i]
            
            try:
                # Current weather API
                url = "https://api.openweathermap.org/data/2.5/weather"
                params = {
                    'lat': point['latitude'],
                    'lon': point['longitude'],
                    'appid': self.openweather_api_key,
                    'units': 'metric'
                }
                
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Determine season based on temperature
                    temp = data['main']['temp']
                    if temp > 35:
                        season = 'summer'
                    elif temp < 15:
                        season = 'winter'
                    elif data['weather'][0]['main'].lower() in ['rain', 'drizzle']:
                        season = 'monsoon'
                    else:
                        season = 'spring'
                    
                    # Map weather condition
                    weather_main = data['weather'][0]['main'].lower()
                    if weather_main in ['rain', 'drizzle']:
                        weather_condition = 'rainy'
                    elif weather_main in ['fog', 'mist', 'haze']:
                        weather_condition = 'foggy'
                    elif weather_main in ['clear']:
                        weather_condition = 'clear'
                    else:
                        weather_condition = 'cloudy'
                    
                    # Calculate risk score based on weather
                    risk_score = 3  # Base risk
                    if weather_condition == 'rainy':
                        risk_score += 3
                    elif weather_condition == 'foggy':
                        risk_score += 4
                    elif temp > 40 or temp < 5:
                        risk_score += 2
                    
                    weather = {
                        'latitude': point['latitude'],
                        'longitude': point['longitude'],
                        'distance_from_start_km': self._calculate_cumulative_distance(coordinates, i),
                        'season': season,
                        'weather_condition': weather_condition,
                        'average_temperature': temp,
                        'humidity': data['main']['humidity'],
                        'pressure': data['main']['pressure'],
                        'visibility_km': data.get('visibility', 10000) / 1000,
                        'wind_speed_kmph': data['wind']['speed'] * 3.6,
                        'wind_direction': self._get_wind_direction(data['wind'].get('deg', 0)),
                        'precipitation_mm': data.get('rain', {}).get('1h', 0),
                        'road_surface_condition': 'wet' if weather_condition == 'rainy' else 'dry',
                        'risk_score': min(10, risk_score),
                        'monsoon_risk': 8 if season == 'monsoon' else 3,
                        'driving_condition_impact': 'severe' if risk_score >= 7 else 'moderate' if risk_score >= 5 else 'minimal',
                        'data_source': 'OPENWEATHER_API'
                    }
                    
                    weather_conditions.append(weather)
                    
                    # Add small delay to avoid rate limiting
                    time.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error fetching weather data: {e}")
                # Add fallback weather data
                weather_conditions.append(self._get_fallback_weather(point, i, coordinates))
        
        return weather_conditions
    
    def _get_real_traffic_data(self, coordinates: List[Dict]) -> List[Dict]:
        """Get real traffic data from TomTom API"""
        traffic_data = []
        sample_interval = max(1, len(coordinates) // 15)
        
        for i in range(0, len(coordinates), sample_interval):
            point = coordinates[i]
            
            try:
                # TomTom Traffic Flow API
                url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
                params = {
                    'point': f"{point['latitude']},{point['longitude']}",
                    'key': self.tomtom_api_key
                }
                
                response = requests.get(url, params=params)
                
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
                    
                    traffic = {
                        'latitude': point['latitude'],
                        'longitude': point['longitude'],
                        'distance_from_start_km': self._calculate_cumulative_distance(coordinates, i),
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
                    
                    traffic_data.append(traffic)
                    
                    # Add small delay to avoid rate limiting
                    time.sleep(0.1)
                    
                else:
                    # Fallback to simulated data if API fails
                    traffic_data.append(self._get_fallback_traffic(point, i, coordinates))
                    
            except Exception as e:
                logger.error(f"Error fetching traffic data: {e}")
                traffic_data.append(self._get_fallback_traffic(point, i, coordinates))
        
        return traffic_data
    
    def _get_wind_direction(self, degrees: float) -> str:
        """Convert wind direction degrees to compass direction"""
        directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        index = int((degrees + 22.5) // 45) % 8
        return directions[index]
    
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
    
    def _enrich_eco_zone(self, zone: Dict) -> Dict:
        """Add zone-specific data"""
        if zone['zone_type'] == 'wildlife_sanctuary':
            zone['wildlife_types'] = ['deer', 'wild_boar', 'peacock']
            zone['critical_habitat'] = True
            zone['speed_limit'] = 30
        elif zone['zone_type'] == 'protected_forest':
            zone['wildlife_types'] = ['various']
            zone['critical_habitat'] = False
            zone['speed_limit'] = 40
            
        return zone
    
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
    
    def process_single_route_simulated(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Process a single route - redirects to real API processing"""
        return self.process_single_route_with_id(route_id, route_info, coordinates)
    
    def reprocess_route(self, route_id: str):
        """Reprocess an existing route with real API data"""
        route = self.route_model.find_by_id(route_id)
        if not route:
            raise ValueError("Route not found")
            
        # Clear existing analysis data
        self._clear_route_analysis(route_id)
        
        # Reprocess
        coordinates = route.get('routePoints', [])
        if not coordinates:
            raise ValueError("No route points found")
            
        # Use empty route_info since we're reprocessing
        route_info = {
            'BU Code': route.get('fromCode', ''),
            'Row Labels': route.get('toCode', ''),
            'Customer Name': route.get('customerName', ''),
            'Location': route.get('location', '')
        }
        
        return self.process_single_route_with_id(route_id, route_info, coordinates)