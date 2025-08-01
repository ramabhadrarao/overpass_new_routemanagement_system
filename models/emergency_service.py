from datetime import datetime
from bson import ObjectId

class EmergencyService:
    collection_name = 'emergencyservices'  # PDF uses this
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('distanceFromRouteKm', 1)])
        
    def create_emergency_service(self, route_id, service_data):
        """Create emergency service with all required fields"""
        service_doc = {
            'routeId': ObjectId(route_id),
            'serviceType': service_data['service_type'],  # hospital, police, fire_station, etc.
            'name': service_data['name'],
            'latitude': service_data['latitude'],
            'longitude': service_data['longitude'],
            'phoneNumber': service_data.get('phone', 'Not available'),
            'address': service_data.get('address', ''),
            'distanceFromRouteKm': service_data['distance_from_route_km'],
            'distanceFromStartKm': service_data.get('distance_from_start_km', 0),
            'distanceFromEndKm': service_data.get('distance_from_end_km', 0),
            'operatingHours': service_data.get('opening_hours', ''),
            'website': service_data.get('website', ''),
            'servicesOffered': service_data.get('services_offered', []),
            'responseTimeMinutes': service_data.get('response_time', 15),
            'availabilityScore': service_data.get('availability_score', 7),
            'priority': service_data.get('priority', 'medium'),
            'dataSource': service_data.get('data_source', 'OVERPASS_API'),
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(service_doc)