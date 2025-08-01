class EcoSensitiveZone:
    collection_name = 'ecosensitivezones'
    
    def create_eco_zone(self, route_id, zone_data):
        """Create eco sensitive zone with all required fields"""
        zone_doc = {
            'routeId': ObjectId(route_id),
            'latitude': zone_data['latitude'],
            'longitude': zone_data['longitude'],
            'zoneType': zone_data['zone_type'],
            'name': zone_data['name'],
            'distanceFromStartKm': zone_data.get('distance_from_start_km', 0),
            'distanceFromEndKm': zone_data.get('distance_from_end_km', 0),
            'distanceFromRouteKm': zone_data['distance_from_route_km'],
            'severity': zone_data['severity'],
            'riskScore': zone_data['risk_score'],
            'restrictions': zone_data['restrictions'],
            'speedLimit': zone_data.get('speed_limit', 40),
            'timingRestrictions': zone_data.get('timing_restrictions', ''),
            'permitRequired': zone_data.get('permit_required', False),
            'wildlifeTypes': zone_data.get('wildlife_types', []),
            'migrationPeriod': zone_data.get('migration_period', ''),
            'criticalHabitat': zone_data.get('critical_habitat', False),
            'dataSource': zone_data.get('data_source', 'OVERPASS_API'),
            'confidence': zone_data.get('confidence', 0.8),
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(zone_doc)