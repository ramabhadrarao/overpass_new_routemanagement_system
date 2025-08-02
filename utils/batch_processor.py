# utils/batch_processor.py
# Utility for batch processing large numbers of routes
# Path: /utils/batch_processor.py

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from pymongo import MongoClient
from bson import ObjectId

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from services.route_processor import RouteProcessor
from utils.file_parser import FileParser

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, mongodb_uri: str = None):
        self.config = Config()
        self.mongodb_uri = mongodb_uri or self.config.MONGODB_URI
        self.client = MongoClient(self.mongodb_uri, maxPoolSize=self.config.MONGODB_POOL_SIZE)
        self.db = self.client.get_database()
        self.file_parser = FileParser()
        
        # Statistics
        self.stats = {
            'total_routes': 0,
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
        
    def process_all_pending_routes(self, max_workers: int = None):
        """Process all pending routes in the database"""
        max_workers = max_workers or self.config.CONCURRENT_WORKERS
        
        # Get all pending routes
        pending_routes = list(self.db.routes.find({
            'processing_status': {'$in': ['pending', 'failed']}
        }))
        
        self.stats['total_routes'] = len(pending_routes)
        self.stats['start_time'] = datetime.utcnow()
        
        logger.info(f"Starting batch processing of {len(pending_routes)} routes with {max_workers} workers")
        
        # Process in batches
        batch_size = self.config.BATCH_SIZE
        
        for i in range(0, len(pending_routes), batch_size):
            batch = pending_routes[i:i + batch_size]
            self._process_batch(batch)
            
            # Progress update
            progress = ((i + len(batch)) / len(pending_routes)) * 100
            logger.info(f"Progress: {progress:.1f}% ({i + len(batch)}/{len(pending_routes)})")
        
        self.stats['end_time'] = datetime.utcnow()
        self._print_summary()
        
    def process_csv_file(self, csv_path: str, route_data_folder: str = 'route_data'):
        """Process routes from a CSV file"""
        logger.info(f"Processing CSV file: {csv_path}")
        
        # Parse CSV
        routes = self.file_parser.parse_route_csv(csv_path)
        self.stats['total_routes'] = len(routes)
        self.stats['start_time'] = datetime.utcnow()
        
        for route_info in routes:
            try:
                # Check if route already exists
                existing_route = self.db.routes.find_one({
                    'fromCode': route_info['BU Code'],
                    'toCode': route_info['Row Labels']
                })
                
                if existing_route and existing_route.get('processing_status') == 'completed':
                    self.stats['skipped'] += 1
                    logger.info(f"Skipping existing route: {route_info['BU Code']} to {route_info['Row Labels']}")
                    continue
                
                # Find coordinate file
                coord_file = self.file_parser.find_coordinate_file(
                    route_info['BU Code'],
                    route_info['Row Labels'],
                    route_data_folder
                )
                
                if not coord_file:
                    self.stats['failed'] += 1
                    logger.error(f"Coordinate file not found for route: {route_info['BU Code']} to {route_info['Row Labels']}")
                    continue
                
                # Parse coordinates
                coordinates = self.file_parser.parse_coordinate_file(coord_file)
                if not coordinates:
                    self.stats['failed'] += 1
                    logger.error(f"No valid coordinates for route: {route_info['BU Code']} to {route_info['Row Labels']}")
                    continue
                
                # Create or update route
                route_data = self._prepare_route_data(route_info, coordinates)
                
                if existing_route:
                    route_id = str(existing_route['_id'])
                    self.db.routes.update_one(
                        {'_id': existing_route['_id']},
                        {'$set': route_data}
                    )
                else:
                    result = self.db.routes.insert_one(route_data)
                    route_id = str(result.inserted_id)
                
                # Process the route
                self._process_single_route(route_id, route_info, coordinates)
                self.stats['processed'] += 1
                
            except Exception as e:
                self.stats['failed'] += 1
                logger.error(f"Error processing route {route_info['BU Code']} to {route_info['Row Labels']}: {e}")
        
        self.stats['end_time'] = datetime.utcnow()
        self._print_summary()
    
    def _process_batch(self, routes: List[Dict]):
        """Process a batch of routes"""
        for route in routes:
            try:
                route_id = str(route['_id'])
                route_info = {
                    'BU Code': route.get('fromCode', ''),
                    'Row Labels': route.get('toCode', ''),
                    'Customer Name': route.get('customerName', ''),
                    'Location': route.get('location', '')
                }
                coordinates = route.get('routePoints', [])
                
                if not coordinates:
                    self.stats['failed'] += 1
                    logger.error(f"No coordinates for route {route_id}")
                    continue
                
                self._process_single_route(route_id, route_info, coordinates)
                self.stats['processed'] += 1
                
            except Exception as e:
                self.stats['failed'] += 1
                logger.error(f"Error processing route {route.get('routeName', 'unknown')}: {e}")
    
    def _process_single_route(self, route_id: str, route_info: Dict, coordinates: List[Dict]):
        """Process a single route"""
        # Create a new database connection for this process
        client = MongoClient(self.mongodb_uri)
        db = client.get_database()
        
        # Create route processor
        processor = RouteProcessor(db, self.config.OVERPASS_API_URL)
        
        try:
            # Process using fast mode
            processor.process_single_route_fast(route_id, route_info, coordinates)
            logger.info(f"Successfully processed route {route_id}")
        except Exception as e:
            logger.error(f"Failed to process route {route_id}: {e}")
            raise
        finally:
            client.close()
    
    def _prepare_route_data(self, route_info: Dict, coordinates: List[Dict]) -> Dict:
        """Prepare route data for database"""
        total_distance = self._calculate_total_distance(coordinates)
        
        return {
            'routeName': f"{route_info['BU Code']}_to_{route_info['Row Labels']}",
            'fromCode': route_info['BU Code'],
            'toCode': route_info['Row Labels'],
            'customerName': route_info.get('Customer Name', ''),
            'location': route_info.get('Location', ''),
            'fromCoordinates': {
                'type': 'Point',
                'coordinates': [coordinates[0]['longitude'], coordinates[0]['latitude']]
            },
            'toCoordinates': {
                'type': 'Point',
                'coordinates': [coordinates[-1]['longitude'], coordinates[-1]['latitude']]
            },
            'fromAddress': f"{route_info['BU Code']} Location",
            'toAddress': f"{route_info['Row Labels']} Location",
            'routePoints': coordinates,
            'totalDistance': total_distance,
            'totalWaypoints': len(coordinates),
            'estimatedDuration': (total_distance / 40) * 60,
            'majorHighways': ['NH-XX', 'SH-YY'],
            'terrain': 'mixed',
            'processing_status': 'pending',
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
    
    def _calculate_total_distance(self, coordinates: List[Dict]) -> float:
        """Calculate total distance for a route"""
        total = 0
        for i in range(1, len(coordinates)):
            # Simple distance calculation
            lat1, lng1 = coordinates[i-1]['latitude'], coordinates[i-1]['longitude']
            lat2, lng2 = coordinates[i]['latitude'], coordinates[i]['longitude']
            
            # Haversine formula
            R = 6371  # Earth's radius in km
            lat_diff = math.radians(lat2 - lat1)
            lng_diff = math.radians(lng2 - lng1)
            
            a = math.sin(lat_diff/2)**2 + math.cos(math.radians(lat1)) * \
                math.cos(math.radians(lat2)) * math.sin(lng_diff/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            
            total += R * c
            
        return round(total, 2)
    
    def _print_summary(self):
        """Print processing summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*50)
        print("BATCH PROCESSING SUMMARY")
        print("="*50)
        print(f"Total Routes: {self.stats['total_routes']}")
        print(f"Processed: {self.stats['processed']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Average: {duration/self.stats['total_routes']:.2f} sec/route" if self.stats['total_routes'] > 0 else "N/A")
        print("="*50)

def main():
    """Main function for command-line usage"""
    import argparse
    import math
    
    parser = argparse.ArgumentParser(description='Batch process routes')
    parser.add_argument('--csv', help='CSV file path to process')
    parser.add_argument('--pending', action='store_true', help='Process all pending routes in database')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers')
    parser.add_argument('--mongodb', help='MongoDB URI', default=Config.MONGODB_URI)
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = BatchProcessor(args.mongodb)
    
    if args.csv:
        processor.process_csv_file(args.csv)
    elif args.pending:
        processor.process_all_pending_routes(args.workers)
    else:
        print("Please specify either --csv or --pending")
        parser.print_help()

if __name__ == '__main__':
    main()