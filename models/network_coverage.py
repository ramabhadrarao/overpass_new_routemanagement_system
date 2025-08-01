from datetime import datetime
from bson import ObjectId

class NetworkCoverage:
    collection_name = 'networkcoverages'  # PDF generator expects this name
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('isDeadZone', 1)])
        self.collection.create_index([('distanceFromStartKm', 1)])
        
    def create_network_coverage(self, route_id, coverage_data):
        """Create network coverage record with all fields expected by PDF generator"""
        coverage_doc = {
            'routeId': ObjectId(route_id),
            'latitude': coverage_data['latitude'],
            'longitude': coverage_data['longitude'],
            'distanceFromStartKm': coverage_data['distance_from_start_km'],
            'distanceFromEndKm': coverage_data.get('distance_from_end_km', 0),
            
            # Coverage Analysis
            'isDeadZone': coverage_data['is_dead_zone'],
            'signalStrength': coverage_data['signal_strength'],  # 1-10 scale
            'signalCategory': coverage_data['signal_category'],  # no_signal, weak, good
            'communicationRisk': coverage_data.get('communication_risk', 'medium'),
            'deadZoneSeverity': coverage_data.get('dead_zone_severity', 'moderate'),
            
            # Provider Information
            'providers': coverage_data.get('providers', ['Airtel', 'Jio', 'Vi']),
            'operatorCoverage': {
                'airtel': {
                    'coverage': coverage_data.get('airtel_coverage', 70),
                    'signalStrength': coverage_data.get('airtel_signal', 5),
                    'technology': coverage_data.get('airtel_tech', '4G')
                },
                'jio': {
                    'coverage': coverage_data.get('jio_coverage', 80),
                    'signalStrength': coverage_data.get('jio_signal', 6),
                    'technology': coverage_data.get('jio_tech', '4G')
                },
                'vi': {
                    'coverage': coverage_data.get('vi_coverage', 60),
                    'signalStrength': coverage_data.get('vi_signal', 4),
                    'technology': coverage_data.get('vi_tech', '4G')
                },
                'bsnl': {
                    'coverage': coverage_data.get('bsnl_coverage', 40),
                    'signalStrength': coverage_data.get('bsnl_signal', 3),
                    'technology': coverage_data.get('bsnl_tech', '3G')
                }
            },
            
            # Geographic Factors
            'terrain': coverage_data.get('terrain', 'rural'),
            'elevation': coverage_data.get('elevation', 0),
            'populationDensity': coverage_data.get('population_density', 'medium'),
            
            # Risk Assessment
            'emergencyRisk': coverage_data.get('emergency_risk', 5),
            
            # Alternative Methods
            'alternativeMethods': coverage_data.get('alternative_methods', []),
            
            # Metadata
            'analysisMethod': coverage_data.get('analysis_method', 'terrain_analysis'),
            'confidence': coverage_data.get('confidence', 0.7),
            'dataSource': coverage_data.get('data_source', 'NETWORK_ANALYSIS'),
            'lastUpdated': datetime.utcnow(),
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(coverage_doc)
    
    def get_route_network_coverage(self, route_id):
        """Get all network coverage points for a route"""
        return list(self.collection.find({'routeId': ObjectId(route_id)})
                   .sort('distanceFromStartKm', 1))
    
    def get_dead_zones(self, route_id):
        """Get only dead zones for a route"""
        return list(self.collection.find({
            'routeId': ObjectId(route_id),
            'isDeadZone': True
        }).sort('distanceFromStartKm', 1))