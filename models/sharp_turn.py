from datetime import datetime
from bson import ObjectId

class SharpTurn:
    collection_name = 'sharpturns'  # Note: PDF uses 'sharpturns' not 'sharp_turns'
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('riskScore', -1)])
        self.collection.create_index([('location', '2dsphere')])
        
    def create_sharp_turn(self, route_id, turn_data):
        """Create a sharp turn record with all fields expected by PDF generator"""
        turn_doc = {
            'routeId': ObjectId(route_id),
            'location': {
                'type': 'Point',
                'coordinates': [turn_data['longitude'], turn_data['latitude']]
            },
            'latitude': turn_data['latitude'],
            'longitude': turn_data['longitude'],
            'turnAngle': turn_data['turn_angle'],  # PDF expects camelCase
            'turnDirection': turn_data['turn_direction'],
            'turnRadius': turn_data.get('turn_radius', 0),
            'turnSeverity': turn_data.get('turn_severity', 'moderate'),  # PDF needs this
            'riskScore': turn_data['risk_score'],
            'distanceFromStartKm': turn_data['distance_from_start_km'],
            'distanceFromEndKm': turn_data.get('distance_from_end_km', 0),  # PDF needs this
            'approachSpeed': turn_data.get('approach_speed', 60),
            'recommendedSpeed': turn_data['recommended_speed'],
            'visibility': turn_data.get('visibility', 'moderate'),
            'roadSurface': turn_data.get('road_surface', 'good'),  # PDF needs this
            'warningSignsPresent': turn_data.get('warning_signs_present', False),
            'guardrailsPresent': turn_data.get('guardrails_present', False),  # PDF needs this
            'guardrails': turn_data.get('guardrails', False),  # Both formats
            'warningSigns': turn_data.get('warning_signs', False),  # Both formats
            'driverActionRequired': turn_data.get('driver_action_required', ''),
            'streetViewImage': turn_data.get('street_view_image'),  # PDF expects this
            'satelliteImage': turn_data.get('satellite_image'),  # PDF expects this
            'roadmapImage': turn_data.get('roadmap_image'),  # PDF expects this
            'dataSource': turn_data.get('data_source', 'OVERPASS_API'),
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(turn_doc)