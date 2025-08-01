from datetime import datetime
from bson import ObjectId

class AccidentProneArea:
    collection_name = 'accidentproneareas'  # PDF uses this
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('riskScore', -1)])
        
    def create_accident_area(self, route_id, area_data):
        """Create accident prone area with all required fields"""
        area_doc = {
            'routeId': ObjectId(route_id),
            'latitude': area_data['latitude'],
            'longitude': area_data['longitude'],
            'distanceFromStartKm': area_data['distance_from_start_km'],
            'distanceFromEndKm': area_data.get('distance_from_end_km', 0),
            'accidentFrequencyYearly': area_data.get('accident_frequency_yearly', 5),
            'accidentSeverity': area_data.get('severity_level', 'moderate'),
            'commonAccidentTypes': area_data.get('accident_types', []),
            'contributingFactors': area_data.get('contributing_factors', []),
            'timeOfDayRisk': area_data.get('time_of_day_risk', {
                'night': 5, 'day': 3, 'peak': 4
            }),
            'weatherRelatedRisk': area_data.get('weather_related_risk', 5),
            'infrastructureRisk': area_data.get('infrastructure_risk', 5),
            'trafficVolumeRisk': area_data.get('traffic_volume_risk', 5),
            'riskScore': area_data['risk_score'],
            'dataSource': area_data.get('data_source', 'ANALYSIS'),
            'dataQuality': area_data.get('data_quality', 'medium'),
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(area_doc)