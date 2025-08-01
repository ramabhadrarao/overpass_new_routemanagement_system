from datetime import datetime
from bson import ObjectId

class Route:
    collection_name = 'routes'
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('route_name', 1)])
        self.collection.create_index([('from_code', 1), ('to_code', 1)])
        self.collection.create_index([('created_at', -1)])
        self.collection.create_index([('processing_status', 1)])
        
    def create_route(self, route_data):
        """Create a new route with all PDF required fields"""
        route_doc = {
            'routeName': route_data['route_name'],  # PDF expects camelCase
            'fromCode': route_data['from_code'],
            'toCode': route_data['to_code'],
            'fromAddress': route_data.get('from_address', ''),
            'toAddress': route_data.get('to_address', ''),
            'fromCoordinates': route_data.get('from_coordinates'),
            'toCoordinates': route_data.get('to_coordinates'),
            'customerName': route_data.get('customer_name', ''),
            'location': route_data.get('location', ''),
            'totalDistance': route_data.get('total_distance', 0),
            'estimatedDuration': route_data.get('estimated_duration', 0),
            'routePoints': route_data.get('route_points', []),
            'totalWaypoints': len(route_data.get('route_points', [])),
            'majorHighways': route_data.get('major_highways', ['NH-XX', 'SH-YY']),  # PDF needs this
            'terrain': route_data.get('terrain', 'mixed'),  # PDF needs this
            'processing_status': 'pending',
            'processing_started_at': None,
            'processing_completed_at': None,
            'processing_errors': [],
            'pdf_generated': False,
            'pdf_path': None,
            'risk_scores': {
                'sharp_turns': 0,
                'blind_spots': 0,
                'accident_prone': 0,
                'road_conditions': 0,
                'emergency_services': 0,
                'network_coverage': 0,
                'overall': 0,
                'risk_level': 'LOW'  # PDF expects this
            },
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        
        return self.collection.insert_one(route_doc)
    
    def find_by_id(self, route_id):
        """Find route by ID"""
        return self.collection.find_one({'_id': ObjectId(route_id)})
    
    def find_by_codes(self, from_code, to_code):
        """Find route by from and to codes"""
        return self.collection.find_one({
            'from_code': from_code,
            'to_code': to_code
        })
    
    def get_all_routes(self, skip=0, limit=20):
        """Get all routes with pagination"""
        return list(self.collection.find()
                   .sort('created_at', -1)
                   .skip(skip)
                   .limit(limit))
    
    def update_processing_status(self, route_id, status, error=None):
        """Update route processing status"""
        update_doc = {
            'processing_status': status,
            'updated_at': datetime.utcnow()
        }
        
        if status == 'processing':
            update_doc['processing_started_at'] = datetime.utcnow()
        elif status == 'completed':
            update_doc['processing_completed_at'] = datetime.utcnow()
        elif status == 'failed' and error:
            update_doc['processing_errors'] = error
            
        return self.collection.update_one(
            {'_id': ObjectId(route_id)},
            {'$set': update_doc}
        )
    
    def update_risk_scores(self, route_id, risk_scores):
        """Update route risk scores"""
        return self.collection.update_one(
            {'_id': ObjectId(route_id)},
            {
                '$set': {
                    'risk_scores': risk_scores,
                    'updated_at': datetime.utcnow()
                }
            }
        )
    
    def mark_pdf_generated(self, route_id, pdf_path):
        """Mark PDF as generated"""
        return self.collection.update_one(
            {'_id': ObjectId(route_id)},
            {
                '$set': {
                    'pdf_generated': True,
                    'pdf_path': pdf_path,
                    'updated_at': datetime.utcnow()
                }
            }
        )