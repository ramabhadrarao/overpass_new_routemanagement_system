#!/usr/bin/env python3
"""
Enhanced test script for ERA5 Seasonal Weather API with debugging
Handles cases where data_available is False
"""

import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Optional
import sys
import os

# Configuration
ERA5_API_KEY = "h4DSeoxB88OwRw7rh42sWJlx8BphPHCi"
ERA5_BASE_URL = "http://43.250.40.133:6000"

# Test route coordinates (Mumbai to Delhi route sample)
TEST_COORDINATES = [
    {"latitude": 19.0760, "longitude": 72.8777, "name": "Mumbai_Start"},
    {"latitude": 21.1458, "longitude": 73.7263, "name": "Nashik"},
    {"latitude": 23.0225, "longitude": 74.5671, "name": "Indore"},
    {"latitude": 26.9124, "longitude": 75.7873, "name": "Jaipur"},
    {"latitude": 28.6139, "longitude": 77.2090, "name": "Delhi_End"}
]

# Alternative test coordinates (in case above don't have data)
ALTERNATIVE_COORDINATES = [
    {"latitude": 28.7041, "longitude": 77.1025, "name": "Delhi_NCR"},
    {"latitude": 19.0760, "longitude": 72.8777, "name": "Mumbai"},
    {"latitude": 13.0827, "longitude": 80.2707, "name": "Chennai"},
    {"latitude": 22.5726, "longitude": 88.3639, "name": "Kolkata"},
    {"latitude": 12.9716, "longitude": 77.5946, "name": "Bangalore"}
]

class EnhancedWeatherTester:
    def __init__(self):
        self.api_key = ERA5_API_KEY
        self.base_url = ERA5_BASE_URL
        self.test_results = []
        
    def test_info_endpoint(self):
        """Test the info endpoint to see available data"""
        print("\n" + "="*60)
        print("TEST: API Info Endpoint")
        print("="*60)
        
        try:
            response = requests.get(f"{self.base_url}/api/weather/info", timeout=5)
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                info = response.json()
                print(f"\nAvailable Data Information:")
                print(f"  Total Fields: {info.get('total_fields', 'N/A')}")
                print(f"  Date Range: {info.get('date_range', {}).get('start', 'N/A')} to {info.get('date_range', {}).get('end', 'N/A')}")
                print(f"  Available Dates: {len(info.get('available_dates', []))}")
                if info.get('available_dates'):
                    print(f"    First few dates: {info['available_dates'][:5]}")
                print(f"  Parameters: {', '.join(info.get('parameters', []))}")
                return info
            else:
                print(f"ERROR: {response.text[:200]}")
                return None
                
        except Exception as e:
            print(f"ERROR: {e}")
            return None
    
    def test_single_point_detailed(self, lat: float, lon: float, date: Optional[int] = None):
        """Test single point with detailed debugging"""
        print("\n" + "="*60)
        print(f"TEST: Single Point Detailed (lat: {lat}, lon: {lon})")
        if date:
            print(f"Date: {date}")
        print("="*60)
        
        try:
            headers = {'X-API-Key': self.api_key}
            params = {'latitude': lat, 'longitude': lon}
            if date:
                params['date'] = date
            
            response = requests.get(
                f"{self.base_url}/api/weather/point",
                params=params,
                headers=headers,
                timeout=10
            )
            
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"\nFull Response:")
                print(json.dumps(data, indent=2))
                
                # Analyze response
                print(f"\nResponse Analysis:")
                print(f"  Data Available: {data.get('data_available', False)}")
                print(f"  Has Temperature: {'temperature' in data}")
                print(f"  Fields Present: {list(data.keys())}")
                
                return data
            else:
                print(f"ERROR Response: {response.text[:500]}")
                return None
                
        except Exception as e:
            print(f"ERROR: {e}")
            return None
    
    def test_seasonal_with_fallback(self, coordinates: List[Dict]):
        """Test seasonal API with better error handling"""
        print("\n" + "="*60)
        print(f"TEST: Seasonal Weather API Enhanced ({len(coordinates)} coordinates)")
        print("="*60)
        
        try:
            headers = {
                'Content-Type': 'application/json',
                'X-API-Key': self.api_key
            }
            
            payload = {"coordinates": coordinates}
            
            response = requests.post(
                f"{self.base_url}/api/weather/route/seasonal",
                json=payload,
                headers=headers,
                timeout=30
            )
            
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                seasonal_data = response.json()
                
                # Save full response for debugging
                with open('seasonal_response_debug.json', 'w') as f:
                    json.dump(seasonal_data, f, indent=2)
                print(f"📄 Full response saved to: seasonal_response_debug.json")
                
                # Analyze each season
                print(f"\nDetailed Season Analysis:")
                data_points_with_values = 0
                data_points_without_values = 0
                
                for season_idx, season_data in enumerate(seasonal_data):
                    if isinstance(season_data, dict):
                        season_name = season_data.get('season', 'unknown')
                        route_weather = season_data.get('route_weather', [])
                        
                        print(f"\n{season_name.upper()} Season:")
                        print(f"  Total Points: {len(route_weather)}")
                        
                        # Check data availability
                        points_with_data = 0
                        points_without_data = 0
                        
                        for point in route_weather:
                            if point.get('data_available', False):
                                points_with_data += 1
                                data_points_with_values += 1
                            else:
                                points_without_data += 1
                                data_points_without_values += 1
                        
                        print(f"  Points with data: {points_with_data}")
                        print(f"  Points without data: {points_without_data}")
                        
                        # Show sample point
                        if route_weather:
                            sample = route_weather[0]
                            print(f"  Sample Point:")
                            print(f"    Name: {sample.get('name', 'N/A')}")
                            print(f"    Data Available: {sample.get('data_available', False)}")
                            print(f"    Temperature: {sample.get('temperature', 'NOT PRESENT')}")
                            print(f"    Fields: {list(sample.keys())}")
                
                print(f"\n📊 Summary:")
                print(f"  Total data points: {data_points_with_values + data_points_without_values}")
                print(f"  Points with data: {data_points_with_values}")
                print(f"  Points without data: {data_points_without_values}")
                
                if data_points_without_values > 0:
                    print(f"\n⚠️  WARNING: {data_points_without_values} points have no data available!")
                    print("  This might be due to:")
                    print("  1. Coordinates outside the GRIB data coverage area")
                    print("  2. Missing data for the specified dates")
                    print("  3. GRIB file not containing data for these locations")
                
                return seasonal_data
            else:
                print(f"ERROR Response: {response.text[:500]}")
                return None
                
        except Exception as e:
            print(f"ERROR: {e}")
            return None
    
    def find_working_coordinates(self):
        """Find coordinates that have actual data"""
        print("\n" + "="*60)
        print("TEST: Finding Coordinates with Available Data")
        print("="*60)
        
        working_coords = []
        
        # Test a grid of coordinates
        test_points = [
            {"lat": 28.6139, "lon": 77.2090, "name": "Delhi"},
            {"lat": 19.0760, "lon": 72.8777, "name": "Mumbai"},
            {"lat": 12.9716, "lon": 77.5946, "name": "Bangalore"},
            {"lat": 22.5726, "lon": 88.3639, "name": "Kolkata"},
            {"lat": 23.0225, "lon": 72.5714, "name": "Ahmedabad"},
            {"lat": 17.3850, "lon": 78.4867, "name": "Hyderabad"},
            {"lat": 26.9124, "lon": 75.7873, "name": "Jaipur"},
            {"lat": 30.7333, "lon": 76.7794, "name": "Chandigarh"},
            {"lat": 11.0168, "lon": 76.9558, "name": "Coimbatore"},
            {"lat": 21.1702, "lon": 72.8311, "name": "Surat"}
        ]
        
        print(f"Testing {len(test_points)} locations...")
        
        for point in test_points:
            headers = {'X-API-Key': self.api_key}
            params = {'latitude': point['lat'], 'longitude': point['lon']}
            
            try:
                response = requests.get(
                    f"{self.base_url}/api/weather/point",
                    params=params,
                    headers=headers,
                    timeout=5
                )
                
                if response.status_code == 200:
                    data = response.json()
                    has_data = data.get('data_available', False)
                    has_temp = 'temperature' in data and data['temperature'] is not None
                    
                    status = "✅ HAS DATA" if (has_data or has_temp) else "❌ NO DATA"
                    print(f"  {point['name']}: {status}")
                    
                    if has_data or has_temp:
                        working_coords.append({
                            "latitude": point['lat'],
                            "longitude": point['lon'],
                            "name": point['name']
                        })
                        
            except Exception as e:
                print(f"  {point['name']}: ERROR - {e}")
        
        print(f"\nFound {len(working_coords)} locations with data")
        return working_coords
    
    def test_raw_data_endpoint(self):
        """Test if there's a raw data endpoint"""
        print("\n" + "="*60)
        print("TEST: Checking Raw Data Access")
        print("="*60)
        
        # Try to get raw data from a known good coordinate
        lat, lon = 28.6139, 77.2090  # Delhi
        
        endpoints_to_try = [
            f"/api/weather/raw?lat={lat}&lon={lon}",
            f"/api/weather/debug?lat={lat}&lon={lon}",
            f"/api/weather/point?latitude={lat}&longitude={lon}&debug=true"
        ]
        
        for endpoint in endpoints_to_try:
            print(f"\nTrying: {self.base_url}{endpoint}")
            try:
                response = requests.get(
                    f"{self.base_url}{endpoint}",
                    headers={'X-API-Key': self.api_key},
                    timeout=5
                )
                print(f"  Status: {response.status_code}")
                if response.status_code == 200:
                    print(f"  Response preview: {str(response.json())[:200]}...")
            except Exception as e:
                print(f"  Error: {e}")
    
    def run_diagnostic_tests(self):
        """Run comprehensive diagnostic tests"""
        print("\n" + "#"*60)
        print("ERA5 WEATHER API DIAGNOSTIC TEST SUITE")
        print(f"Server: {self.base_url}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("#"*60)
        
        # 1. Check API info
        info = self.test_info_endpoint()
        
        # 2. Test single point with different dates
        if info and info.get('available_dates'):
            # Test with first available date
            first_date = info['available_dates'][0]
            print(f"\nTesting with first available date: {first_date}")
            self.test_single_point_detailed(28.6139, 77.2090, int(first_date))
        
        # 3. Find working coordinates
        working_coords = self.find_working_coordinates()
        
        # 4. If we found working coordinates, test seasonal API with them
        if working_coords:
            print("\nTesting seasonal API with working coordinates...")
            self.test_seasonal_with_fallback(working_coords[:3])  # Use first 3
        
        # 5. Test with original coordinates for comparison
        print("\nTesting with original Mumbai-Delhi coordinates...")
        self.test_seasonal_with_fallback(TEST_COORDINATES)
        
        # 6. Check for raw data endpoints
        self.test_raw_data_endpoint()
        
        print("\n" + "#"*60)
        print("DIAGNOSTIC COMPLETE")
        print("#"*60)
        print("\nRecommendations:")
        print("1. Check if GRIB file contains data for Indian coordinates")
        print("2. Verify date ranges in GRIB file match the API dates")
        print("3. Consider using coordinates that returned data")
        print("4. Check GRIB file metadata for coverage area")


def main():
    """Main function"""
    tester = EnhancedWeatherTester()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--quick':
        # Quick test with single point
        tester.test_single_point_detailed(28.6139, 77.2090)
    else:
        # Run full diagnostics
        tester.run_diagnostic_tests()


if __name__ == "__main__":
    main()