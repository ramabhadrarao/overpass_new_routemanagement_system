from datetime import datetime
from bson import ObjectId

class APILog:
    collection_name = 'api_logs'
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('route_id', 1)])
        self.collection.create_index([('api_name', 1)])
        self.collection.create_index([('timestamp', -1)])
        
    def log_api_call(self, route_id, api_name, endpoint, request_data, response_data, 
                     status_code, response_time, error=None):
        """Log an API call"""
        log_doc = {
            'route_id': ObjectId(route_id) if route_id else None,
            'api_name': api_name,
            'endpoint': endpoint,
            'request_data': request_data,
            'response_data': response_data,
            'status_code': status_code,
            'response_time': response_time,  # in milliseconds
            'error': error,
            'timestamp': datetime.utcnow()
        }
        
        return self.collection.insert_one(log_doc)
    
    def get_route_api_logs(self, route_id):
        """Get all API logs for a route"""
        return list(self.collection.find({'route_id': ObjectId(route_id)})
                   .sort('timestamp', -1))
    
    def get_api_stats(self, api_name=None):
        """Get API call statistics"""
        match_stage = {}
        if api_name:
            match_stage['api_name'] = api_name
            
        pipeline = [
            {'$match': match_stage},
            {
                '$group': {
                    '_id': '$api_name',
                    'total_calls': {'$sum': 1},
                    'avg_response_time': {'$avg': '$response_time'},
                    'success_count': {
                        '$sum': {
                            '$cond': [{'$eq': ['$status_code', 200]}, 1, 0]
                        }
                    },
                    'error_count': {
                        '$sum': {
                            '$cond': [{'$ne': ['$error', None]}, 1, 0]
                        }
                    }
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))