import requests
import time
from typing import List, Dict, Tuple
import math
from models.api_log import APILog

class OverpassService:
    def __init__(self, overpass_url, db):
        self.overpass_url = overpass_url
        self.api_log = APILog(db)
        
    def query_overpass(self, query: str, route_id=None) -> dict:
        """Execute an Overpass API query and log it"""
        start_time = time.time()
        
        try:
            response = requests.post(
                self.overpass_url,
                data={'data': query},
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=30
            )
            
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            
            # Log API call
            self.api_log.log_api_call(
                route_id=route_id,
                api_name='overpass',
                endpoint=self.overpass_url,
                request_data={'query': query[:500]},  # Truncate long queries
                response_data={'elements_count': len(response.json().get('elements', []))},
                status_code=response.status_code,
                response_time=response_time
            )
            
            return response.json()
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            # Log error
            self.api_log.log_api_call(
                route_id=route_id,
                api_name='overpass',
                endpoint=self.overpass_url,
                request_data={'query': query[:500]},
                response_data=None,
                status_code=0,
                response_time=response_time,
                error=str(e)
            )
            
            raise e
    
    def get_emergency_services(self, route_id: str, bounds: Dict) -> List[Dict]:
        """Get emergency services along the route"""
        query = f"""
        [out:json][timeout:30];
        (
          node["amenity"="hospital"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["amenity"="hospital"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          node["amenity"="police"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["amenity"="police"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          node["amenity"="fire_station"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["amenity"="fire_station"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          node["amenity"="fuel"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["amenity"="fuel"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          node["amenity"="school"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["amenity"="school"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
        );
        out center;
        """
        
        data = self.query_overpass(query, route_id)
        services = []
        
        for element in data.get('elements', []):
            lat = element.get('lat') or element.get('center', {}).get('lat')
            lon = element.get('lon') or element.get('center', {}).get('lon')
            
            if lat and lon:
                service = {
                    'latitude': lat,
                    'longitude': lon,
                    'service_type': element.get('tags', {}).get('amenity'),
                    'name': element.get('tags', {}).get('name', 'Unknown'),
                    'phone': element.get('tags', {}).get('phone', 'Not available'),
                    'address': element.get('tags', {}).get('addr:full', '') or 
                              f"{element.get('tags', {}).get('addr:street', '')}, {element.get('tags', {}).get('addr:city', '')}",
                    'opening_hours': element.get('tags', {}).get('opening_hours', ''),
                    'website': element.get('tags', {}).get('website', '')
                }
                services.append(service)
                
        return services
    
    def get_road_conditions(self, route_id: str, route_points: List[Dict]) -> List[Dict]:
        """Get road conditions along the route"""
        conditions = []
        
        # Sample every 10th point to avoid too many queries
        sample_interval = max(1, len(route_points) // 20)
        
        for i in range(0, len(route_points), sample_interval):
            point = route_points[i]
            
            # Query for road information around this point
            query = f"""
            [out:json][timeout:25];
            way(around:50,{point['latitude']},{point['longitude']})["highway"];
            out tags;
            """
            
            try:
                data = self.query_overpass(query, route_id)
                
                if data.get('elements'):
                    road = data['elements'][0]
                    tags = road.get('tags', {})
                    
                    condition = {
                        'latitude': point['latitude'],
                        'longitude': point['longitude'],
                        'road_type': tags.get('highway', 'unclassified'),
                        'surface': tags.get('surface', 'unknown'),
                        'lanes': int(tags.get('lanes', 2)),
                        'max_speed': int(tags.get('maxspeed', '60').replace('km/h', '').strip()) if tags.get('maxspeed') else 60,
                        'width': float(tags.get('width', 7)),
                        'under_construction': bool(tags.get('construction')),
                        'surface_quality': self._assess_surface_quality(tags),
                        'risk_score': self._calculate_road_risk(tags),
                        'distance_from_start_km': point.get('distance_from_start', 0)
                    }
                    conditions.append(condition)
                    
            except Exception as e:
                print(f"Error getting road condition: {e}")
                continue
                
        return conditions
    
    def get_eco_sensitive_zones(self, route_id: str, bounds: Dict) -> List[Dict]:
        """Get eco-sensitive zones along the route"""
        query = f"""
        [out:json][timeout:30];
        (
          way["boundary"="protected_area"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["boundary"="national_park"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["natural"="wood"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["landuse"="forest"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
          way["leisure"="nature_reserve"]({bounds['min_lat']},{bounds['min_lng']},{bounds['max_lat']},{bounds['max_lng']});
        );
        out center;
        """
        
        data = self.query_overpass(query, route_id)
        zones = []
        
        for element in data.get('elements', []):
            center = element.get('center', {})
            if center.get('lat') and center.get('lon'):
                zone = {
                    'latitude': center['lat'],
                    'longitude': center['lon'],
                    'zone_type': element.get('tags', {}).get('boundary') or 
                                element.get('tags', {}).get('natural') or 
                                element.get('tags', {}).get('landuse') or 'protected_area',
                    'name': element.get('tags', {}).get('name', 'Unknown Zone'),
                    'severity': 'high' if element.get('tags', {}).get('boundary') == 'national_park' else 'medium',
                    'restrictions': self._get_eco_zone_restrictions(element.get('tags', {})),
                    'risk_score': 7 if element.get('tags', {}).get('boundary') == 'national_park' else 5
                }
                zones.append(zone)
                
        return zones
    
    def _assess_surface_quality(self, tags: Dict) -> str:
        """Assess road surface quality from OSM tags"""
        surface = tags.get('surface', '').lower()
        smoothness = tags.get('smoothness', '').lower()
        
        if surface in ['unpaved', 'dirt', 'gravel', 'ground']:
            return 'poor'
        elif surface in ['compacted', 'fine_gravel']:
            return 'fair'
        elif smoothness in ['bad', 'very_bad', 'horrible', 'very_horrible']:
            return 'poor'
        elif smoothness in ['intermediate']:
            return 'fair'
        elif surface in ['asphalt', 'concrete'] and smoothness in ['excellent', 'good']:
            return 'excellent'
        else:
            return 'good'
    
    def _calculate_road_risk(self, tags: Dict) -> int:
        """Calculate road risk score from OSM tags"""
        risk = 3  # Base risk
        
        surface = tags.get('surface', '').lower()
        if surface in ['unpaved', 'dirt', 'gravel']:
            risk += 3
        elif surface in ['compacted', 'fine_gravel']:
            risk += 1
            
        if tags.get('construction'):
            risk += 3
            
        lanes = int(tags.get('lanes', 2))
        if lanes == 1:
            risk += 2
            
        return min(risk, 10)
    
    def _get_eco_zone_restrictions(self, tags: Dict) -> List[str]:
        """Get restrictions for eco-sensitive zones"""
        restrictions = []
        
        if tags.get('boundary') == 'national_park':
            restrictions.extend(['no_horn', 'speed_limit_40', 'no_stopping'])
        elif tags.get('boundary') == 'protected_area':
            restrictions.extend(['no_horn', 'speed_limit_50'])
            
        if tags.get('access') == 'no':
            restrictions.append('restricted_access')
            
        return restrictions