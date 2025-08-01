from datetime import datetime
from bson import ObjectId

class RoadCondition:
    collection_name = 'roadconditions'
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('riskScore', -1)])
        
    def create_road_condition(self, route_id, condition_data):
        """Create road condition with all required fields"""
        condition_doc = {
            'routeId': ObjectId(route_id),
            'latitude': condition_data['latitude'],
            'longitude': condition_data['longitude'],
            'roadType': condition_data['road_type'],
            'surfaceQuality': condition_data['surface_quality'],
            'widthMeters': condition_data.get('width', 7),
            'laneCount': condition_data.get('lanes', 2),
            'hasPotholes': condition_data.get('has_potholes', False),
            'underConstruction': condition_data.get('under_construction', False),
            'riskScore': condition_data['risk_score'],
            'distanceFromStartKm': condition_data.get('distance_from_start_km', 0),
            'surface': condition_data.get('surface', 'asphalt'),  # PDF might need this
            'dataSource': condition_data['data_source'],
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(condition_doc)