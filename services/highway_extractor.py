# services/highway_extractor.py
# Service to extract major highways from route coordinates
# Path: /services/highway_extractor.py

import logging
from typing import List, Dict, Set
from collections import Counter

logger = logging.getLogger(__name__)

class HighwayExtractor:
    def __init__(self, overpass_service):
        self.overpass_service = overpass_service
        
    def extract_highways_from_route(self, route_id: str, coordinates: List[Dict]) -> List[str]:
        """Extract major highways that the route passes through"""
        highways = set()
        highway_counts = Counter()
        
        # Sample points along the route (every 5-10 km)
        sample_interval = max(1, len(coordinates) // 20)
        sample_points = coordinates[::sample_interval]
        
        logger.info(f"Extracting highways from {len(sample_points)} sample points")
        
        for point in sample_points:
            try:
                # Query for highways near this point
                query = f"""
                [out:json][timeout:10];
                way(around:100,{point['latitude']},{point['longitude']})["highway"]["ref"];
                out tags;
                """
                
                data = self.overpass_service.query_overpass(query, route_id)
                
                for element in data.get('elements', []):
                    tags = element.get('tags', {})
                    highway_ref = tags.get('ref', '')
                    highway_type = tags.get('highway', '')
                    
                    # Process highway references
                    if highway_ref:
                        # Handle multiple refs (e.g., "NH 44; SH 7")
                        refs = [ref.strip() for ref in highway_ref.replace(',', ';').split(';')]
                        for ref in refs:
                            if ref and self._is_major_highway(ref, highway_type):
                                highway_counts[ref] += 1
                                
            except Exception as e:
                logger.error(f"Error querying highways at point: {e}")
                continue
        
        # Sort highways by frequency (most frequently encountered first)
        major_highways = [hw for hw, count in highway_counts.most_common() if count >= 2]
        
        # If no highways found, return default
        if not major_highways:
            logger.warning("No major highways found, using defaults")
            return ['State Highway', 'District Road']
            
        logger.info(f"Found highways: {major_highways}")
        return major_highways[:10]  # Return top 10 most frequent highways
    
    def _is_major_highway(self, ref: str, highway_type: str) -> bool:
        """Check if this is a major highway"""
        # National Highways (NH), State Highways (SH), Major District Roads (MDR)
        major_prefixes = ['NH', 'SH', 'MDR', 'AH']  # AH = Asian Highway
        
        # Check if ref starts with major prefix
        ref_upper = ref.upper()
        for prefix in major_prefixes:
            if ref_upper.startswith(prefix):
                return True
                
        # Check if it's a major road type
        major_types = ['trunk', 'primary', 'secondary', 'motorway']
        if highway_type in major_types:
            return True
            
        return False
    
    def extract_highways_simple(self, coordinates: List[Dict]) -> List[str]:
        """Simple extraction without API calls - for fallback"""
        # This is a simplified version that returns generic highway types
        # based on route distance
        total_distance = self._calculate_route_distance(coordinates)
        
        if total_distance > 500:
            return ['National Highway', 'State Highway', 'Major District Road']
        elif total_distance > 200:
            return ['State Highway', 'Major District Road']
        elif total_distance > 100:
            return ['State Highway', 'District Road']
        else:
            return ['District Road', 'Local Road']
    
    def _calculate_route_distance(self, coordinates: List[Dict]) -> float:
        """Calculate total route distance"""
        total = 0
        for i in range(1, len(coordinates)):
            # Simple distance calculation
            lat1, lng1 = coordinates[i-1]['latitude'], coordinates[i-1]['longitude']
            lat2, lng2 = coordinates[i]['latitude'], coordinates[i]['longitude']
            
            # Approximate distance
            lat_diff = abs(lat2 - lat1)
            lng_diff = abs(lng2 - lng1)
            distance = ((lat_diff ** 2 + lng_diff ** 2) ** 0.5) * 111  # rough km conversion
            total += distance
            
        return total