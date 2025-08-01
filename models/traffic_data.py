from datetime import datetime
from bson import ObjectId

class TrafficData:
    collection_name = 'trafficdata'  # PDF generator expects this name
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('congestionLevel', 1)])
        self.collection.create_index([('distanceFromStartKm', 1)])
        
    def create_traffic_data(self, route_id, traffic_data):
        """Create traffic data record with all fields expected by PDF generator"""
        traffic_doc = {
            'routeId': ObjectId(route_id),
            'latitude': traffic_data['latitude'],
            'longitude': traffic_data['longitude'],
            'distanceFromStartKm': traffic_data['distance_from_start_km'],
            'distanceFromEndKm': traffic_data.get('distance_from_end_km', 0),
            
            # Traffic Flow Data
            'averageSpeedKmph': traffic_data['average_speed_kmph'],
            'congestionLevel': traffic_data['congestion_level'],  # free_flow, light, moderate, heavy, severe
            'peakHourTrafficCount': traffic_data.get('peak_hour_traffic_count', 0),
            
            # Road Infrastructure
            'speedLimit': traffic_data.get('speed_limit', 60),
            'roadType': traffic_data.get('road_type', 'highway'),
            'trafficLights': traffic_data.get('traffic_lights', 0),
            'tollPoints': traffic_data.get('toll_points', 0),
            'constructionZones': traffic_data.get('construction_zones', 0),
            
            # Traffic Issues
            'bottleneckCauses': traffic_data.get('bottleneck_causes', []),
            'alternativeRoutesAvailable': traffic_data.get('alternative_routes', False),
            
            # Risk Assessment
            'riskScore': traffic_data['risk_score'],
            
            # Timing Information
            'measurementTime': datetime.utcnow(),
            'timeOfDay': traffic_data.get('time_of_day', 'afternoon'),
            
            # Additional Metrics
            'accidentReports': traffic_data.get('accident_reports', 0),
            'weatherImpact': traffic_data.get('weather_impact', 'none'),
            'specialEvents': traffic_data.get('special_events', []),
            
            # Data Quality
            'dataSource': traffic_data.get('data_source', 'TRAFFIC_ANALYSIS'),
            'confidence': traffic_data.get('confidence', 0.8),
            
            # Timestamps
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(traffic_doc)
    
    def get_route_traffic_data(self, route_id):
        """Get all traffic data points for a route"""
        return list(self.collection.find({'routeId': ObjectId(route_id)})
                   .sort('distanceFromStartKm', 1))
    
    def get_congestion_areas(self, route_id, min_congestion='moderate'):
        """Get congested areas for a route"""
        congestion_levels = ['free_flow', 'light', 'moderate', 'heavy', 'severe']
        min_index = congestion_levels.index(min_congestion)
        
        return list(self.collection.find({
            'routeId': ObjectId(route_id),
            'congestionLevel': {'$in': congestion_levels[min_index:]}
        }).sort('congestionLevel', -1))