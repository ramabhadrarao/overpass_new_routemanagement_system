#!/usr/bin/env python3
"""
Test script for WeatherService
Tests the weather service independently
"""

import sys
import os
import logging
from pymongo import MongoClient

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.weather_service import WeatherService

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_weather_service():
    """Test the weather service with sample coordinates"""
    
    # Initialize MongoDB connection
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/route_risk_management_2')
    client = MongoClient(mongodb_uri)
    db = client.get_database()
    
    # Initialize weather service
    weather_service = WeatherService(db)
    
    # Test coordinates (your sample route)
    test_coordinates = [
        {"latitude": 25.6824, "longitude": 88.0713},  # Point A
        {"latitude": 26.2175, "longitude": 88.1329},  # Point B
        {"latitude": 21.3884, "longitude": 81.6700},  # Point C
    ]
    
    # Test route ID (you can use a real one from your database)
    test_route_id = "test_route_001"
    
    print("=" * 80)
    print("TESTING WEATHER SERVICE")
    print("=" * 80)
    print(f"Test coordinates: {len(test_coordinates)} points")
    print(f"Test route ID: {test_route_id}")
    print()
    
    try:
        # Call weather service
        print("Calling weather service...")
        weather_conditions = weather_service.get_route_weather_data(
            route_id=test_route_id,
            coordinates=test_coordinates
        )
        
        print(f"\nResults: {len(weather_conditions)} weather conditions retrieved")
        
        # Analyze results
        if weather_conditions:
            # Group by season
            seasons_data = {}
            locations_data = {}
            
            for wc in weather_conditions:
                season = wc.get('season', 'unknown')
                if season not in seasons_data:
                    seasons_data[season] = []
                seasons_data[season].append(wc)
                
                # Track locations
                loc_key = f"{wc['latitude']:.4f},{wc['longitude']:.4f}"
                if loc_key not in locations_data:
                    locations_data[loc_key] = []
                locations_data[loc_key].append(season)
            
            print("\nSEASONAL BREAKDOWN:")
            print("-" * 40)
            for season, conditions in sorted(seasons_data.items()):
                print(f"{season.upper()}: {len(conditions)} conditions")
                
                # Show sample data
                if conditions:
                    sample = conditions[0]
                    print(f"  Sample data:")
                    print(f"    Temperature: {sample.get('average_temperature')}°C")
                    print(f"    Humidity: {sample.get('humidity')}%")
                    print(f"    Precipitation: {sample.get('precipitation_mm')}mm")
                    print(f"    Weather: {sample.get('weather_condition')}")
                    print(f"    Risk Score: {sample.get('risk_score')}")
            
            print("\nLOCATION COVERAGE:")
            print("-" * 40)
            for loc, seasons in locations_data.items():
                print(f"{loc}: {len(set(seasons))} unique seasons")
                print(f"  Seasons: {', '.join(sorted(set(seasons)))}")
            
            # Check data completeness
            print("\nDATA COMPLETENESS CHECK:")
            print("-" * 40)
            required_fields = [
                'season', 'weather_condition', 'average_temperature',
                'humidity', 'precipitation_mm', 'visibility_km',
                'wind_speed_kmph', 'risk_score'
            ]
            
            complete_records = 0
            for wc in weather_conditions:
                if all(field in wc and wc[field] is not None for field in required_fields):
                    complete_records += 1
            
            print(f"Complete records: {complete_records}/{len(weather_conditions)}")
            
            # Show first complete record
            print("\nSAMPLE COMPLETE RECORD:")
            print("-" * 40)
            if weather_conditions:
                sample = weather_conditions[0]
                for key, value in sorted(sample.items()):
                    if not key.startswith('_') and key not in ['createdAt', 'updatedAt']:
                        print(f"{key}: {value}")
            
        else:
            print("\n⚠️  No weather conditions returned!")
            print("Check the API logs in MongoDB for errors")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Check API logs
        print("\nCHECKING API LOGS:")
        print("-" * 40)
        
        api_logs = list(db.api_logs.find({
            'api_name': {'$in': ['era5_seasonal_weather', 'era5_point_weather']}
        }).sort('timestamp', -1).limit(5))
        
        for log in api_logs:
            print(f"{log['timestamp']} - {log['api_name']}: Status {log['status_code']}, Time: {log['response_time']}ms")
        
        client.close()

def test_single_location():
    """Test weather service for different coordinate sets"""
    
    # Different test routes
    test_routes = [
        {
            'name': 'Mumbai to Delhi',
            'coordinates': [
                {"latitude": 19.0760, "longitude": 72.8777},  # Mumbai
                {"latitude": 21.1458, "longitude": 79.0882},  # Nagpur
                {"latitude": 26.9124, "longitude": 75.7873},  # Jaipur
                {"latitude": 28.6139, "longitude": 77.2090},  # Delhi
            ]
        },
        {
            'name': 'Kolkata to Chennai',
            'coordinates': [
                {"latitude": 22.5726, "longitude": 88.3639},  # Kolkata
                {"latitude": 20.2961, "longitude": 85.8245},  # Bhubaneswar
                {"latitude": 17.3850, "longitude": 78.4867},  # Hyderabad
                {"latitude": 13.0827, "longitude": 80.2707},  # Chennai
            ]
        }
    ]
    
    # Initialize MongoDB connection
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/route_risk_management_2')
    client = MongoClient(mongodb_uri)
    db = client.get_database()
    
    # Initialize weather service
    weather_service = WeatherService(db)
    
    for route in test_routes:
        print(f"\n{'='*60}")
        print(f"Testing route: {route['name']}")
        print(f"{'='*60}")
        
        try:
            weather_conditions = weather_service.get_route_weather_data(
                route_id=f"test_{route['name'].replace(' ', '_')}",
                coordinates=route['coordinates']
            )
            
            print(f"Retrieved {len(weather_conditions)} weather conditions")
            
            # Summary by season
            season_summary = {}
            for wc in weather_conditions:
                season = wc.get('season', 'unknown')
                season_summary[season] = season_summary.get(season, 0) + 1
            
            print("Conditions by season:", season_summary)
            
        except Exception as e:
            print(f"Error: {e}")
    
    client.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Weather Service')
    parser.add_argument('--test-routes', action='store_true', 
                       help='Test multiple routes')
    
    args = parser.parse_args()
    
    if args.test_routes:
        test_single_location()
    else:
        test_weather_service()