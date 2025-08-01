from datetime import datetime
from bson import ObjectId

class BlindSpot:
    collection_name = 'blindspots'  # PDF uses 'blindspots'
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('riskScore', -1)])
        
    def create_blind_spot(self, route_id, spot_data):
        """Create a blind spot record with all fields expected by PDF generator"""
        spot_doc = {
            'routeId': ObjectId(route_id),
            'latitude': spot_data['latitude'],
            'longitude': spot_data['longitude'],
            'spotType': spot_data['spot_type'],
            'visibilityDistance': spot_data['visibility_distance'],
            'riskScore': spot_data['risk_score'],
            'distanceFromStartKm': spot_data['distance_from_start_km'],
            'distanceFromEndKm': spot_data.get('distance_from_end_km', 0),
            'driverActionRequired': spot_data.get('driver_action_required', ''),
            'obstructionHeight': spot_data.get('obstruction_height', 0),
            'roadGeometry': {
                'gradient': spot_data.get('gradient', 0),
                'curvature': spot_data.get('curvature', 0),
                'width': spot_data.get('road_width', 7)
            },
            'vegetation': spot_data.get('vegetation', {}),
            'warningSignsPresent': spot_data.get('warning_signs_present', False),
            'mirrorInstalled': spot_data.get('mirror_installed', False),
            'dataSource': spot_data.get('data_source', 'OVERPASS_API'),
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(spot_doc)