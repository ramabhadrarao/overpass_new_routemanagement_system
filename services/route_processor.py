# services/route_processor.py
import os
from datetime import datetime
from typing import Dict, List, Optional
from services.overpass_service import OverpassService
from services.risk_calculator import RiskCalculator
from utils.file_parser import FileParser
from models import *

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
        
    def process_csv_file(self, csv_path: str, route_data_folder: str) -> Dict:
        """Process CSV file and all routes within it"""
        routes = self.file_parser.parse_route_csv(csv_path)
        results = {
            'total': len(routes),
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'routes': []
        }
        
        for route_info in routes:
            # Check if route already exists
            existing_route = self.route_model.find_by_codes(
                route_info['BU Code'],
                route_info['Row Labels']
            )
            
            if existing_route and existing_route['processing_status'] == 'completed':
                results['skipped'] += 1
                results['routes'].append({
                    'route_id': str(existing_route['_id']),
                    'status': 'skipped',
                    'message': 'Route already processed'
                })
                continue
                
            # Process the route
            try:
                route_result = self.process_single_route(route_info, route_data_folder)
                results['processed'] += 1
                results['routes'].append(route_result)
            except Exception as e:
                results['failed'] += 1
                results['routes'].append({
                    'route_id': None,
                    'status': 'failed',
                    'error': str(e)
                })
                
        return results
    
    def process_single_route(self, route_info: Dict, route_data_folder: str) -> Dict:
        """Process a single route"""
        # Find coordinate file
        coord_file = self.file_parser.find_coordinate_file(
            route_info['BU Code'],
            route_info['Row Labels'],
            route_data_folder
        )
        
        if not coord_file:
            raise ValueError(f"Coordinate file not found for {route_info['BU Code']}_{route_info['Row Labels']}")
            
        # Parse coordinates
        coordinates = self.file_parser.parse_coordinate_file(coord_file)
        if not coordinates:
            raise ValueError("No valid coordinates found in file")
            
        # Calculate route distance
        total_distance = self._calculate_total_distance(coordinates)
        
        # Prepare route data
        route_data = {
            'route_name': f"{route_info['BU Code']}_to_{route_info['Row Labels']}",
            'from_code': route_info['BU Code'],
            'to_code': route_info['Row Labels'],
            'customer_name': route_info.get('Customer Name', ''),
            'location': route_info.get('Location', ''),
            'from_coordinates': {
                'latitude': coordinates[0]['latitude'],
                'longitude': coordinates[0]['longitude']
            },
            'to_coordinates': {
                'latitude': coordinates[-1]['latitude'],
                'longitude': coordinates[-1]['longitude']
            },
            'route_points': coordinates,
            'total_distance': total_distance,
            'estimated_duration': (total_distance / 40) * 60,  # Assuming 40 km/h average
            'from_address': f"{route_info['BU Code']} Location",  # Would geocode in real app
            'to_address': f"{route_info['Row Labels']} Location",
            'major_highways': ['NH-XX', 'SH-YY'],  # Would extract from route
            'terrain': 'mixed'  # Would analyze from elevation data
        }
        
        # Create route in database
        route_result = self.route_model.create_route(route_data)
        route_id = str(route_result.inserted_id)
        
        # Update processing status
        self.route_model.update_processing_status(route_id, 'processing')
        
        try:
            # Process route analysis
            analysis_results = self._analyze_route(route_id, coordinates)
            
            # Calculate risk scores
            risk_scores = self.risk_calculator.calculate_overall_risk_score(analysis_results)
            
            # Update route with risk scores
            self.route_model.update_risk_scores(route_id, risk_scores['scores'])
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
            
        self.route_model.update_processing_status(route_id, 'processing')
        
        try:
            analysis_results = self._analyze_route(route_id, coordinates)
            risk_scores = self.risk_calculator.calculate_overall_risk_score(analysis_results)
            self.route_model.update_risk_scores(route_id, risk_scores['scores'])
            self.route_model.update_processing_status(route_id, 'completed')
        except Exception as e:
            self.route_model.update_processing_status(route_id, 'failed', str(e))
            raise e
    
    def _analyze_route(self, route_id: str, coordinates: List[Dict]) -> Dict:
        """Analyze route for various risk factors"""
        results = {}
        
        # Calculate total route distance
        total_distance = self._calculate_total_distance(coordinates)
        
        # Calculate bounds for Overpass queries
        bounds = self._calculate_bounds(coordinates)
        
        # 1. Analyze sharp turns
        sharp_turns = self.risk_calculator.analyze_sharp_turns(coordinates)
        for turn in sharp_turns:
            turn['distance_from_end_km'] = total_distance - turn['distance_from_start_km']
            turn['turn_severity'] = self._get_turn_severity(turn['turn_angle'])
            turn['road_surface'] = 'good'  # Would need actual data
            turn['guardrails'] = False
            turn['warning_signs'] = False
            
            self.sharp_turn_model.create_sharp_turn(route_id, turn)
        results['sharp_turns'] = sharp_turns
        
        # 2. Identify blind spots
        blind_spots = self.risk_calculator.identify_blind_spots(coordinates)
        for spot in blind_spots:
            spot['distance_from_end_km'] = total_distance - spot['distance_from_start_km']
            spot['gradient'] = 0
            spot['curvature'] = 0
            spot['road_width'] = 7
            
            self.blind_spot_model.create_blind_spot(route_id, spot)
        results['blind_spots'] = blind_spots
        
        # 3. Get emergency services
        emergency_services = self.overpass_service.get_emergency_services(route_id, bounds)
        for service in emergency_services:
            service['distance_from_start_km'] = self._calculate_distance_along_route(
                service['latitude'], service['longitude'], coordinates
            )
            service['distance_from_end_km'] = total_distance - service['distance_from_start_km']
            service = self._enrich_emergency_service(service)
            
            self.emergency_service_model.create_emergency_service(route_id, service)
        results['emergency_services'] = emergency_services
        
        # 4. Get road conditions
        road_conditions = self.overpass_service.get_road_conditions(route_id, coordinates)
        for condition in road_conditions:
            condition['has_potholes'] = condition['surface_quality'] in ['poor', 'critical']
            condition['lanes'] = condition.get('lanes', 2)
            
            self.road_condition_model.create_road_condition(route_id, condition)
        results['road_conditions'] = road_conditions
        
        # 5. Analyze network coverage
        network_coverage = self.risk_calculator.analyze_network_coverage(coordinates)
        for coverage in network_coverage:
            coverage['distance_from_end_km'] = total_distance - coverage['distance_from_start_km']
            coverage['dead_zone_severity'] = 'critical' if coverage['is_dead_zone'] else 'none'
            coverage['communication_risk'] = 8 if coverage['is_dead_zone'] else 3
            
            self.network_coverage_model.create_network_coverage(route_id, coverage)
        results['network_coverage'] = network_coverage
        
        # 6. Get eco-sensitive zones
        eco_zones = self.overpass_service.get_eco_sensitive_zones(route_id, bounds)
        for zone in eco_zones:
            zone['distance_from_start_km'] = self._calculate_distance_along_route(
                zone['latitude'], zone['longitude'], coordinates
            )
            zone['distance_from_end_km'] = total_distance - zone['distance_from_start_km']
            zone = self._enrich_eco_zone(zone)
            
            self.eco_zone_model.create_eco_zone(route_id, zone)
        results['eco_sensitive_zones'] = eco_zones
        
        # 7. Identify accident-prone areas
        accident_areas = self._identify_accident_prone_areas(sharp_turns, road_conditions)
        for area in accident_areas:
            area['distance_from_end_km'] = total_distance - area['distance_from_start_km']
            area = self._enrich_accident_area(area)
            
            self.accident_prone_model.create_accident_area(route_id, area)
        results['accident_prone_areas'] = accident_areas
        
        # 8. Add weather conditions (simulated)
        weather_conditions = self._simulate_weather_conditions(coordinates)
        for weather in weather_conditions:
            self.weather_condition_model.create_weather_condition(route_id, weather)
        results['weather_conditions'] = weather_conditions
        
        # 9. Add traffic data (simulated)
        traffic_data = self._simulate_traffic_data(coordinates)
        for traffic in traffic_data:
            self.traffic_data_model.create_traffic_data(route_id, traffic)
        results['traffic_data'] = traffic_data
        
        return results
    
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
    
    def _simulate_weather_conditions(self, coordinates: List[Dict]) -> List[Dict]:
        """Simulate weather conditions for PDF requirements"""
        weather_conditions = []
        sample_interval = max(1, len(coordinates) // 10)
        
        seasons = ['summer', 'monsoon', 'winter', 'spring']
        conditions = ['clear', 'rainy', 'foggy', 'cloudy']
        
        for i in range(0, len(coordinates), sample_interval):
            point = coordinates[i]
            season = seasons[i % len(seasons)]
            
            weather = {
                'latitude': point['latitude'],
                'longitude': point['longitude'],
                'distance_from_start_km': self._calculate_cumulative_distance(coordinates, i),
                'season': season,
                'weather_condition': conditions[i % len(conditions)],
                'average_temperature': 25 + (i % 15),
                'humidity': 60 + (i % 30),
                'visibility_km': 10 - (i % 8),
                'wind_speed_kmph': 10 + (i % 20),
                'road_surface_condition': 'dry' if season != 'monsoon' else 'wet',
                'risk_score': 3 + (i % 5),
                'monsoon_risk': 8 if season == 'monsoon' else 3,
                'driving_condition_impact': 'moderate' if season in ['monsoon', 'winter'] else 'minimal'
            }
            
            weather_conditions.append(weather)
        
        return weather_conditions
    
    def _simulate_traffic_data(self, coordinates: List[Dict]) -> List[Dict]:
        """Simulate traffic data for PDF requirements"""
        traffic_data = []
        sample_interval = max(1, len(coordinates) // 15)
        
        congestion_levels = ['free_flow', 'light', 'moderate', 'heavy']
        
        for i in range(0, len(coordinates), sample_interval):
            point = coordinates[i]
            congestion_index = i % len(congestion_levels)
            
            traffic = {
                'latitude': point['latitude'],
                'longitude': point['longitude'],
                'distance_from_start_km': self._calculate_cumulative_distance(coordinates, i),
                'average_speed_kmph': 60 - (congestion_index * 10),
                'congestion_level': congestion_levels[congestion_index],
                'risk_score': 2 + (congestion_index * 2)
            }
            
            traffic_data.append(traffic)
        
        return traffic_data
    
    def _clear_route_analysis(self, route_id: str):
        """Clear existing analysis data for a route"""
        from bson import ObjectId
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