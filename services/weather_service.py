# services/weather_service.py
# Dedicated service for handling weather data from ERA5 API
# Path: /services/weather_service.py

import requests
import time
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from models.weather_condition import WeatherCondition
from models.api_log import APILog

logger = logging.getLogger(__name__)

class WeatherService:
    """Service for fetching and processing weather data from ERA5 API"""
    
    def __init__(self, db, api_key: str = None, api_url: str = None):
        """Initialize weather service"""
        self.db = db
        self.weather_model = WeatherCondition(db)
        self.api_log = APILog(db)
        
        # API configuration
        self.api_key = api_key or "h4DSeoxB88OwRw7rh42sWJlx8BphPHCi"
        self.seasonal_api_url = api_url or "http://43.250.40.133:6000/api/weather/route/seasonal"
        self.point_api_url = "http://43.250.40.133:6000/api/weather/point"
        
        # Request configuration
        self.timeout = 30
        self.max_retries = 2
        
        logger.info(f"WeatherService initialized with seasonal API: {self.seasonal_api_url}")
    
    def get_route_weather_data(self, route_id: str, coordinates: List[Dict], 
                             sample_points: Optional[List[int]] = None) -> List[Dict]:
        """
        Get weather data for a route using seasonal API
        
        Args:
            route_id: Route identifier
            coordinates: List of route coordinates
            sample_points: Optional list of indices to sample (default: smart sampling)
            
        Returns:
            List of weather condition dictionaries saved to database
        """
        logger.info(f"Getting weather data for route {route_id} with {len(coordinates)} coordinates")
        
        # Determine sampling points
        if sample_points is None:
            sample_points = self._get_sample_indices(len(coordinates))
        
        logger.info(f"Sampling {len(sample_points)} points for weather data")
        
        # Prepare coordinates for API
        api_coordinates = []
        for idx in sample_points:
            if idx < len(coordinates):
                point = coordinates[idx]
                api_coordinates.append({
                    "latitude": point['latitude'],
                    "longitude": point['longitude'],
                    "name": f"Point_{idx}"
                })
        
        # Call seasonal API
        seasonal_data = self._call_seasonal_api(api_coordinates, route_id)
        
        if not seasonal_data:
            logger.error("Failed to get seasonal weather data")
            return []
        
        # Process and save weather data
        weather_conditions = self._process_and_save_seasonal_data(
            seasonal_data, 
            sample_points, 
            coordinates, 
            route_id
        )
        
        logger.info(f"Successfully processed {len(weather_conditions)} weather conditions")
        return weather_conditions
    
    def _get_sample_indices(self, total_points: int) -> List[int]:
        """Get smart sample indices based on route length"""
        if total_points <= 3:
            return list(range(total_points))
        elif total_points <= 5:
            return list(range(total_points))
        else:
            # Sample up to 5 points: start, 1/4, middle, 3/4, end
            return [
                0,
                total_points // 4,
                total_points // 2,
                3 * total_points // 4,
                total_points - 1
            ]
    
    def _call_seasonal_api(self, coordinates: List[Dict], route_id: str) -> Optional[List[Dict]]:
        """Call the seasonal weather API"""
        payload = {"coordinates": coordinates}
        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        }
        
        logger.debug(f"Calling seasonal API with {len(coordinates)} coordinates")
        
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()
                
                response = requests.post(
                    self.seasonal_api_url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout
                )
                
                response_time = (time.time() - start_time) * 1000
                
                # Log API call
                self.api_log.log_api_call(
                    route_id=route_id,
                    api_name='era5_seasonal_weather',
                    endpoint=self.seasonal_api_url,
                    request_data={'coordinates_count': len(coordinates)},
                    response_data={'status_code': response.status_code},
                    status_code=response.status_code,
                    response_time=response_time
                )
                
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"Seasonal API returned data successfully in {response_time:.0f}ms")
                    return data
                else:
                    logger.error(f"Seasonal API error: {response.status_code} - {response.text[:200]}")
                    
            except requests.exceptions.Timeout:
                logger.error(f"Seasonal API timeout (attempt {attempt + 1}/{self.max_retries})")
            except Exception as e:
                logger.error(f"Error calling seasonal API: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def _process_and_save_seasonal_data(self, seasonal_data: List[Dict], 
                                      sample_indices: List[int],
                                      coordinates: List[Dict], 
                                      route_id: str) -> List[Dict]:
        """Process seasonal weather data and save to database"""
        weather_conditions = []
        total_distance = self._calculate_total_distance(coordinates)
        
        # Process each season's data
        for season_entry in seasonal_data:
            if not isinstance(season_entry, dict):
                continue
            
            season = season_entry.get('season', 'unknown')
            route_weather = season_entry.get('route_weather', [])
            avg_season_temp = season_entry.get('average_temperature', 25)
            date_used = season_entry.get('date_used', '')
            
            logger.info(f"Processing {season} season with {len(route_weather)} points")
            
            # Process each weather point
            for point_idx, weather_point in enumerate(route_weather):
                if point_idx >= len(sample_indices):
                    continue
                
                sample_idx = sample_indices[point_idx]
                if sample_idx >= len(coordinates):
                    continue
                
                coord = coordinates[sample_idx]
                
                # Create weather condition
                weather_condition = self._create_weather_condition(
                    weather_point, coord, sample_idx, coordinates,
                    total_distance, season, avg_season_temp, date_used
                )
                
                # Save to database
                try:
                    self.weather_model.create_weather_condition(route_id, weather_condition)
                    weather_conditions.append(weather_condition)
                    logger.debug(f"Saved {season} weather for point {point_idx}")
                except Exception as e:
                    logger.error(f"Error saving weather condition: {e}")
        
        # Log summary
        self._log_weather_summary(weather_conditions)
        
        return weather_conditions
    
    def _create_weather_condition(self, weather_point: Dict, coord: Dict, 
                                sample_idx: int, coordinates: List[Dict],
                                total_distance: float, season: str, 
                                avg_season_temp: float, date_used: str) -> Dict:
        """Create a complete weather condition entry"""
        
        cumulative_distance = self._calculate_cumulative_distance(coordinates, sample_idx)
        
        # Extract all weather data with proper field mapping
        weather_condition = {
            # Location data
            'latitude': coord['latitude'],
            'longitude': coord['longitude'],
            'distance_from_start_km': cumulative_distance,
            'distance_from_end_km': total_distance - cumulative_distance,
            
            # Season data
            'season': season,
            'date_used': date_used,
            
            # Temperature data
            'average_temperature': weather_point.get('temperature', avg_season_temp),
            'averageTemperature': weather_point.get('temperature', avg_season_temp),
            
            # Weather conditions
            'weather_condition': weather_point.get('weather_condition', 'unknown'),
            'weatherCondition': weather_point.get('weather_condition', 'unknown'),
            
            # Atmospheric data
            'humidity': weather_point.get('humidity', 0),
            'pressure': weather_point.get('pressure', 1013),
            'dewpoint': weather_point.get('dewpoint', 0),
            'cloud_cover': weather_point.get('cloud_cover', 0),
            
            # Visibility
            'visibility_km': weather_point.get('visibility_km', 10),
            'visibilityKm': weather_point.get('visibility_km', 10),
            
            # Wind data
            'wind_speed_kmph': weather_point.get('wind_speed_kmph', 0),
            'windSpeedKmph': weather_point.get('wind_speed_kmph', 0),
            'wind_direction': weather_point.get('wind_direction', 'N'),
            'windDirection': weather_point.get('wind_direction', 'N'),
            
            # Precipitation
            'precipitation_mm': weather_point.get('precipitation', 0),
            'precipitationMm': weather_point.get('precipitation', 0),
            
            # Road conditions
            'road_surface_condition': weather_point.get('road_surface_condition', 'unknown'),
            'roadSurfaceCondition': weather_point.get('road_surface_condition', 'unknown'),
            
            # Risk assessment
            'risk_score': weather_point.get('risk_score', 5),
            'riskScore': weather_point.get('risk_score', 5),
            'monsoon_risk': self._calculate_monsoon_risk(season, weather_point),
            'monsoonRisk': self._calculate_monsoon_risk(season, weather_point),
            
            # Driving impact
            'driving_condition_impact': weather_point.get('driving_condition_impact', 'unknown'),
            'drivingConditionImpact': weather_point.get('driving_condition_impact', 'unknown'),
            
            # Additional fields from API
            'uv_index': weather_point.get('uv_index', 5),
            'uvIndex': weather_point.get('uv_index', 5),
            'extreme_weather_history': [],
            'extremeWeatherHistory': [],
            'recommended_precautions': self._get_precautions_for_season(season),
            'recommendedPrecautions': self._get_precautions_for_season(season),
            
            # Data quality
            'data_available': weather_point.get('data_available', True),
            'dataAvailable': weather_point.get('data_available', True),
            'data_source': 'ERA5_SEASONAL_API',
            'dataSource': 'ERA5_SEASONAL_API',
            'data_year': datetime.now().year,
            'dataYear': datetime.now().year,
            'forecast_accuracy': 85,
            'forecastAccuracy': 85
        }
        
        return weather_condition
    
    def _calculate_monsoon_risk(self, season: str, weather_point: Dict) -> int:
        """Calculate monsoon risk based on season and conditions"""
        if season == 'monsoon':
            base_risk = 7
            if weather_point.get('precipitation', 0) > 50:
                base_risk = 9
            elif weather_point.get('precipitation', 0) > 20:
                base_risk = 8
            return base_risk
        return 3
    
    def _get_precautions_for_season(self, season: str) -> List[str]:
        """Get safety precautions based on season"""
        precautions = {
            'winter': [
                'Check visibility conditions',
                'Watch for fog in early morning',
                'Maintain moderate speed'
            ],
            'summer': [
                'Carry sufficient water',
                'Check vehicle cooling system',
                'Avoid peak afternoon hours if possible'
            ],
            'monsoon': [
                'Reduce speed on wet roads',
                'Maintain safe following distance',
                'Check tire condition and pressure',
                'Avoid waterlogged areas',
                'Use headlights in heavy rain'
            ],
            'autumn': [
                'Watch for changing weather',
                'Be prepared for occasional showers'
            ],
            'spring': [
                'Monitor weather updates',
                'Standard safety precautions'
            ]
        }
        return precautions.get(season, ['Follow standard safety guidelines'])
    
    def _calculate_total_distance(self, coordinates: List[Dict]) -> float:
        """Calculate total route distance in kilometers"""
        total = 0.0
        for i in range(1, len(coordinates)):
            distance = self._haversine_distance(
                coordinates[i-1]['latitude'], coordinates[i-1]['longitude'],
                coordinates[i]['latitude'], coordinates[i]['longitude']
            )
            total += distance
        return round(total, 2)
    
    def _calculate_cumulative_distance(self, coordinates: List[Dict], up_to_index: int) -> float:
        """Calculate cumulative distance up to a specific index"""
        distance = 0.0
        for i in range(min(up_to_index, len(coordinates) - 1)):
            distance += self._haversine_distance(
                coordinates[i]['latitude'], coordinates[i]['longitude'],
                coordinates[i+1]['latitude'], coordinates[i+1]['longitude']
            )
        return round(distance, 2)
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points using Haversine formula"""
        import math
        
        R = 6371  # Earth's radius in kilometers
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c
    
    def _log_weather_summary(self, weather_conditions: List[Dict]):
        """Log summary of processed weather data"""
        if not weather_conditions:
            logger.warning("No weather conditions processed")
            return
        
        # Count by season
        season_counts = {}
        location_counts = {}
        
        for wc in weather_conditions:
            season = wc.get('season', 'unknown')
            season_counts[season] = season_counts.get(season, 0) + 1
            
            loc_key = f"{wc['latitude']:.4f},{wc['longitude']:.4f}"
            if loc_key not in location_counts:
                location_counts[loc_key] = set()
            location_counts[loc_key].add(season)
        
        logger.info("Weather data summary:")
        logger.info(f"  Total conditions: {len(weather_conditions)}")
        logger.info(f"  Unique locations: {len(location_counts)}")
        logger.info("  Conditions by season:")
        for season, count in sorted(season_counts.items()):
            logger.info(f"    {season}: {count}")
        
        # Check coverage
        complete_locations = sum(1 for seasons in location_counts.values() if len(seasons) >= 3)
        logger.info(f"  Locations with 3+ seasons: {complete_locations}/{len(location_counts)}")
    
    def get_weather_for_single_point(self, latitude: float, longitude: float, 
                                   route_id: Optional[str] = None) -> Optional[Dict]:
        """Get weather data for a single point (fallback method)"""
        params = {
            'latitude': latitude,
            'longitude': longitude
        }
        
        headers = {'X-API-Key': self.api_key}
        
        try:
            start_time = time.time()
            response = requests.get(
                self.point_api_url,
                params=params,
                headers=headers,
                timeout=10
            )
            response_time = (time.time() - start_time) * 1000
            
            if route_id:
                self.api_log.log_api_call(
                    route_id=route_id,
                    api_name='era5_point_weather',
                    endpoint=self.point_api_url,
                    request_data=params,
                    response_data={'status_code': response.status_code},
                    status_code=response.status_code,
                    response_time=response_time
                )
            
            if response.status_code == 200:
                return response.json()
            
        except Exception as e:
            logger.error(f"Error fetching single point weather: {e}")
        
        return None