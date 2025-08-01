import math
from typing import List, Dict, Tuple

class RiskCalculator:
    def __init__(self):
        self.sharp_turn_threshold = 60  # degrees
        
    def calculate_bearing(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate bearing between two points"""
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        lng_diff = math.radians(lng2 - lng1)
        
        y = math.sin(lng_diff) * math.cos(lat2_rad)
        x = math.cos(lat1_rad) * math.sin(lat2_rad) - \
            math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(lng_diff)
        
        bearing = math.degrees(math.atan2(y, x))
        return (bearing + 360) % 360
    
    def calculate_distance(self, lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """Calculate distance between two points in kilometers"""
        R = 6371  # Earth's radius in kilometers
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        lat_diff = math.radians(lat2 - lat1)
        lng_diff = math.radians(lng2 - lng1)
        
        a = math.sin(lat_diff / 2) ** 2 + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(lng_diff / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c
    
    def analyze_sharp_turns(self, route_points: List[Dict]) -> List[Dict]:
        """Analyze route for sharp turns"""
        sharp_turns = []
        cumulative_distance = 0
        
        for i in range(1, len(route_points) - 1):
            p1 = route_points[i - 1]
            p2 = route_points[i]
            p3 = route_points[i + 1]
            
            # Calculate distance
            segment_distance = self.calculate_distance(
                p1['latitude'], p1['longitude'],
                p2['latitude'], p2['longitude']
            )
            cumulative_distance += segment_distance
            
            # Calculate bearings
            bearing1 = self.calculate_bearing(
                p1['latitude'], p1['longitude'],
                p2['latitude'], p2['longitude']
            )
            bearing2 = self.calculate_bearing(
                p2['latitude'], p2['longitude'],
                p3['latitude'], p3['longitude']
            )
            
            # Calculate turn angle
            turn_angle = abs(bearing2 - bearing1)
            if turn_angle > 180:
                turn_angle = 360 - turn_angle
                
            # Check if it's a sharp turn
            if turn_angle > self.sharp_turn_threshold:
                direction = 'right' if bearing2 > bearing1 else 'left'
                
                # Calculate risk score
                if turn_angle > 120:
                    risk_score = 9
                    recommended_speed = 20
                elif turn_angle > 90:
                    risk_score = 8
                    recommended_speed = 25
                elif turn_angle > 75:
                    risk_score = 7
                    recommended_speed = 30
                else:
                    risk_score = 5
                    recommended_speed = 40
                
                sharp_turn = {
                    'latitude': p2['latitude'],
                    'longitude': p2['longitude'],
                    'turn_angle': round(turn_angle, 1),
                    'turn_direction': direction,
                    'risk_score': risk_score,
                    'distance_from_start_km': round(cumulative_distance, 2),
                    'recommended_speed': recommended_speed,
                    'driver_action_required': f"Reduce speed to {recommended_speed} km/h for sharp {direction} turn"
                }
                
                sharp_turns.append(sharp_turn)
                
        return sharp_turns
    
    def identify_blind_spots(self, route_points: List[Dict]) -> List[Dict]:
        """Identify potential blind spots based on route geometry"""
        blind_spots = []
        cumulative_distance = 0
        
        for i in range(2, len(route_points) - 2):
            p1 = route_points[i - 2]
            p2 = route_points[i - 1]
            p3 = route_points[i]
            p4 = route_points[i + 1]
            p5 = route_points[i + 2]
            
            cumulative_distance += self.calculate_distance(
                p2['latitude'], p2['longitude'],
                p3['latitude'], p3['longitude']
            )
            
            # Calculate bearing changes
            bearing1 = self.calculate_bearing(
                p1['latitude'], p1['longitude'],
                p3['latitude'], p3['longitude']
            )
            bearing2 = self.calculate_bearing(
                p3['latitude'], p3['longitude'],
                p5['latitude'], p5['longitude']
            )
            
            bearing_change = abs(bearing2 - bearing1)
            if bearing_change > 180:
                bearing_change = 360 - bearing_change
                
            # Detect potential blind spots
            if bearing_change > 30:
                spot_type = 'sharp_curve' if bearing_change > 60 else 'curve'
                risk_score = 8 if bearing_change > 60 else 6
                visibility_distance = 50 if bearing_change > 60 else 100
                
                blind_spot = {
                    'latitude': p3['latitude'],
                    'longitude': p3['longitude'],
                    'spot_type': spot_type,
                    'visibility_distance': visibility_distance,
                    'risk_score': risk_score,
                    'distance_from_start_km': round(cumulative_distance, 2),
                    'driver_action_required': 'Reduce speed and honk before curve'
                }
                
                blind_spots.append(blind_spot)
                
        return blind_spots
    
    def analyze_network_coverage(self, route_points: List[Dict]) -> List[Dict]:
        """Simulate network coverage analysis"""
        coverage_points = []
        sample_interval = max(1, len(route_points) // 15)
        cumulative_distance = 0
        
        for i in range(0, len(route_points), sample_interval):
            if i > 0:
                cumulative_distance += self.calculate_distance(
                    route_points[i-1]['latitude'], route_points[i-1]['longitude'],
                    route_points[i]['latitude'], route_points[i]['longitude']
                ) * sample_interval
                
            point = route_points[i]
            
            # Simulate coverage based on location (simplified)
            # In reality, this would use actual network data
            is_remote = i % 5 == 0  # Simulate 20% remote areas
            signal_strength = 1 if is_remote else 4
            is_dead_zone = signal_strength < 2
            
            coverage = {
                'latitude': point['latitude'],
                'longitude': point['longitude'],
                'is_dead_zone': is_dead_zone,
                'signal_strength': signal_strength,
                'signal_category': 'no_signal' if is_dead_zone else 'weak' if signal_strength < 3 else 'good',
                'communication_risk': 'high' if is_dead_zone else 'medium' if signal_strength < 3 else 'low',
                'distance_from_start_km': round(cumulative_distance, 2),
                'providers': ['Airtel', 'Jio', 'Vi']
            }
            
            coverage_points.append(coverage)
            
        return coverage_points
    # Add to services/risk_calculator.py

    def analyze_network_coverage(self, route_points: List[Dict]) -> List[Dict]:
        """Simulate network coverage analysis based on terrain and population"""
        coverage_points = []
        sample_interval = max(1, len(route_points) // 15)
        cumulative_distance = 0
        
        for i in range(0, len(route_points), sample_interval):
            if i > 0:
                cumulative_distance += self.calculate_distance(
                    route_points[i-1]['latitude'], route_points[i-1]['longitude'],
                    route_points[i]['latitude'], route_points[i]['longitude']
                ) * sample_interval
                
            point = route_points[i]
            
            # Simulate coverage based on location
            # In reality, this would use terrain analysis and population data
            is_remote = i % 5 == 0  # Simulate 20% remote areas
            signal_strength = 1 if is_remote else 4 + (i % 6)
            is_dead_zone = signal_strength < 2
            
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
                'terrain': 'rural' if is_remote else 'urban',
                'population_density': 'low' if is_remote else 'medium',
                'alternative_methods': ['satellite_phone'] if is_dead_zone else []
            }
            
            coverage_points.append(coverage)
            
        return coverage_points

    def analyze_traffic_patterns(self, route_points: List[Dict]) -> List[Dict]:
        """Simulate traffic data based on route characteristics"""
        traffic_points = []
        sample_interval = max(1, len(route_points) // 20)
        cumulative_distance = 0
        
        congestion_patterns = ['free_flow', 'light', 'moderate', 'heavy', 'light', 'free_flow']
        
        for i in range(0, len(route_points), sample_interval):
            if i > 0:
                cumulative_distance += self.calculate_distance(
                    route_points[i-1]['latitude'], route_points[i-1]['longitude'],
                    route_points[i]['latitude'], route_points[i]['longitude']
                ) * sample_interval
                
            point = route_points[i]
            congestion_index = i % len(congestion_patterns)
            congestion = congestion_patterns[congestion_index]
            
            # Speed based on congestion
            speed_map = {
                'free_flow': 60,
                'light': 50,
                'moderate': 40,
                'heavy': 25,
                'severe': 15
            }
            
            # Risk based on congestion
            risk_map = {
                'free_flow': 2,
                'light': 3,
                'moderate': 5,
                'heavy': 7,
                'severe': 9
            }
            
            traffic = {
                'latitude': point['latitude'],
                'longitude': point['longitude'],
                'distance_from_start_km': round(cumulative_distance, 2),
                'average_speed_kmph': speed_map.get(congestion, 40),
                'congestion_level': congestion,
                'risk_score': risk_map.get(congestion, 5),
                'peak_hour_traffic_count': 100 + (congestion_index * 50),
                'time_of_day': 'afternoon',
                'bottleneck_causes': ['junction'] if congestion == 'heavy' else []
            }
            
            traffic_points.append(traffic)
            
        return traffic_points
    def calculate_overall_risk_score(self, route_data: Dict) -> Dict:
        """Calculate overall risk score for the route"""
        weights = {
            'sharp_turns': 0.25,
            'blind_spots': 0.20,
            'road_conditions': 0.20,
            'emergency_services': 0.15,
            'network_coverage': 0.10,
            'eco_zones': 0.10
        }
        
        scores = {}
        
        # Sharp turns risk
        sharp_turns = route_data.get('sharp_turns', [])
        if sharp_turns:
            avg_turn_risk = sum(t['risk_score'] for t in sharp_turns) / len(sharp_turns)
            scores['sharp_turns'] = avg_turn_risk
        else:
            scores['sharp_turns'] = 2
            
        # Blind spots risk
        blind_spots = route_data.get('blind_spots', [])
        if blind_spots:
            avg_blind_risk = sum(b['risk_score'] for b in blind_spots) / len(blind_spots)
            scores['blind_spots'] = avg_blind_risk
        else:
            scores['blind_spots'] = 2
            
        # Road conditions risk
        road_conditions = route_data.get('road_conditions', [])
        if road_conditions:
            avg_road_risk = sum(r['risk_score'] for r in road_conditions) / len(road_conditions)
            scores['road_conditions'] = avg_road_risk
        else:
            scores['road_conditions'] = 3
            
        # Emergency services (inverse - more services = lower risk)
        emergency_services = route_data.get('emergency_services', [])
        emergency_score = max(1, 10 - len(emergency_services) * 0.5)
        scores['emergency_services'] = emergency_score
        
        # Network coverage risk
        network_coverage = route_data.get('network_coverage', [])
        dead_zones = sum(1 for n in network_coverage if n['is_dead_zone'])
        coverage_risk = min(10, 2 + dead_zones * 2)
        scores['network_coverage'] = coverage_risk
        
        # Eco zones risk
        eco_zones = route_data.get('eco_sensitive_zones', [])
        eco_risk = min(10, 3 + len(eco_zones) * 1.5)
        scores['eco_zones'] = eco_risk
        
        # Calculate weighted overall score
        overall_score = sum(scores[key] * weights.get(key, 0) for key in scores)
        
        # Determine risk level
        if overall_score >= 8:
            risk_level = 'CRITICAL'
        elif overall_score >= 6:
            risk_level = 'HIGH'
        elif overall_score >= 4:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
            
        return {
            'scores': scores,
            'overall': round(overall_score, 2),
            'risk_level': risk_level
        }