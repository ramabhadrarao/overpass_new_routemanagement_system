import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple
import matplotlib.pyplot as plt
import seaborn as sns

class RouteWeatherAnalyzer:
    """Analyze historical weather data along a route for all seasons"""
    
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key
        self.headers = {'X-API-Key': api_key}
        
    def load_route_from_csv(self, csv_file: str) -> List[Dict]:
        """Load route coordinates from CSV file"""
        df = pd.read_csv(csv_file, header=None, names=['latitude', 'longitude'])
        
        # Create route points with names
        route_points = []
        for idx, (lat, lon) in enumerate(df.values):
            route_points.append({
                'latitude': lat,
                'longitude': lon,
                'name': f'Point_{idx+1}'
            })
        
        return route_points
    
    def fetch_seasonal_weather(self, coordinates: List[Dict]) -> Dict:
        """Fetch weather data for all seasons from the API"""
        payload = {'coordinates': coordinates}
        
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None
    
    def parse_weather_data(self, raw_data: Dict) -> pd.DataFrame:
        """Parse weather data into a structured DataFrame"""
        records = []
        
        if not raw_data:
            return pd.DataFrame()
        
        # Handle both list and dict responses
        data = raw_data if isinstance(raw_data, list) else [raw_data]
        
        for seasonal_data in data:
            season = seasonal_data.get('season', 'unknown')
            avg_temp = seasonal_data.get('average_temperature', 0)
            date_used = seasonal_data.get('date_used', '')
            
            for point in seasonal_data.get('route_weather', []):
                record = {
                    'season': season,
                    'date_used': date_used,
                    'average_season_temp': avg_temp,
                    'point_name': point.get('name'),
                    'latitude': point.get('latitude'),
                    'longitude': point.get('longitude'),
                    'temperature': point.get('temperature'),
                    'humidity': point.get('humidity'),
                    'pressure': point.get('pressure'),
                    'precipitation': point.get('precipitation'),
                    'wind_speed': point.get('wind_speed_kmph'),
                    'wind_direction': point.get('wind_direction'),
                    'cloud_cover': point.get('cloud_cover'),
                    'visibility': point.get('visibility_km'),
                    'weather_condition': point.get('weather_condition'),
                    'road_surface': point.get('road_surface_condition'),
                    'risk_score': point.get('risk_score'),
                    'driving_impact': point.get('driving_condition_impact')
                }
                records.append(record)
        
        return pd.DataFrame(records)
    
    def calculate_route_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate statistical summaries for the route"""
        if df.empty:
            return {}
        
        stats = {}
        
        # Overall statistics
        stats['overall'] = {
            'avg_temperature': df['temperature'].mean(),
            'avg_humidity': df['humidity'].mean(),
            'avg_precipitation': df['precipitation'].mean(),
            'avg_wind_speed': df['wind_speed'].mean(),
            'avg_visibility': df['visibility'].mean()
        }
        
        # Seasonal statistics
        stats['seasonal'] = {}
        for season in df['season'].unique():
            season_df = df[df['season'] == season]
            stats['seasonal'][season] = {
                'avg_temperature': season_df['temperature'].mean(),
                'avg_humidity': season_df['humidity'].mean(),
                'avg_precipitation': season_df['precipitation'].mean(),
                'avg_wind_speed': season_df['wind_speed'].mean(),
                'max_precipitation': season_df['precipitation'].max(),
                'min_visibility': season_df['visibility'].min(),
                'weather_conditions': season_df['weather_condition'].value_counts().to_dict(),
                'road_conditions': season_df['road_surface'].value_counts().to_dict()
            }
        
        # Point-wise statistics
        stats['points'] = {}
        for point in df['point_name'].unique():
            point_df = df[df['point_name'] == point]
            stats['points'][point] = {
                'avg_temperature': point_df['temperature'].mean(),
                'temperature_range': (point_df['temperature'].min(), point_df['temperature'].max()),
                'avg_humidity': point_df['humidity'].mean(),
                'total_precipitation': point_df['precipitation'].sum()
            }
        
        return stats
    
    def visualize_weather_patterns(self, df: pd.DataFrame, output_prefix: str = 'weather_analysis'):
        """Create visualizations of weather patterns"""
        if df.empty:
            print("No data to visualize")
            return
        
        # Set up the plotting style
        plt.style.use('seaborn-v0_8-darkgrid')
        
        # 1. Temperature variation across seasons and points
        plt.figure(figsize=(12, 6))
        pivot_temp = df.pivot_table(values='temperature', index='point_name', columns='season')
        pivot_temp.plot(kind='bar', ax=plt.gca())
        plt.title('Temperature Variation Across Route Points by Season')
        plt.xlabel('Route Points')
        plt.ylabel('Temperature (°C)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{output_prefix}_temperature_variation.png')
        plt.close()
        
        # 2. Precipitation patterns
        plt.figure(figsize=(12, 6))
        pivot_precip = df.pivot_table(values='precipitation', index='point_name', columns='season')
        pivot_precip.plot(kind='bar', ax=plt.gca())
        plt.title('Precipitation Patterns Across Route Points by Season')
        plt.xlabel('Route Points')
        plt.ylabel('Precipitation (mm)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{output_prefix}_precipitation_patterns.png')
        plt.close()
        
        # 3. Weather conditions distribution
        plt.figure(figsize=(10, 6))
        weather_counts = df.groupby(['season', 'weather_condition']).size().unstack(fill_value=0)
        weather_counts.plot(kind='bar', stacked=True, ax=plt.gca())
        plt.title('Weather Conditions Distribution by Season')
        plt.xlabel('Season')
        plt.ylabel('Frequency')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{output_prefix}_weather_conditions.png')
        plt.close()
        
        # 4. Risk score analysis
        plt.figure(figsize=(10, 6))
        risk_data = df.groupby(['season', 'point_name'])['risk_score'].mean().unstack()
        risk_data.plot(kind='line', marker='o', ax=plt.gca())
        plt.title('Average Risk Score by Season and Location')
        plt.xlabel('Season')
        plt.ylabel('Risk Score')
        plt.legend(title='Location', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(f'{output_prefix}_risk_analysis.png')
        plt.close()
    
    def generate_report(self, df: pd.DataFrame, stats: Dict, output_file: str = 'weather_report.txt'):
        """Generate a comprehensive weather analysis report"""
        with open(output_file, 'w') as f:
            f.write("ROUTE HISTORICAL WEATHER ANALYSIS REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Data Points: {len(df)}\n")
            f.write(f"Seasons Analyzed: {', '.join(df['season'].unique())}\n\n")
            
            # Overall Statistics
            f.write("OVERALL ROUTE STATISTICS\n")
            f.write("-" * 30 + "\n")
            for metric, value in stats.get('overall', {}).items():
                f.write(f"{metric.replace('_', ' ').title()}: {value:.2f}\n")
            
            # Seasonal Analysis
            f.write("\n\nSEASONAL ANALYSIS\n")
            f.write("-" * 30 + "\n")
            for season, season_stats in stats.get('seasonal', {}).items():
                f.write(f"\n{season.upper()}:\n")
                f.write(f"  Average Temperature: {season_stats['avg_temperature']:.1f}°C\n")
                f.write(f"  Average Humidity: {season_stats['avg_humidity']:.1f}%\n")
                f.write(f"  Average Precipitation: {season_stats['avg_precipitation']:.2f}mm\n")
                f.write(f"  Maximum Precipitation: {season_stats['max_precipitation']:.2f}mm\n")
                f.write(f"  Minimum Visibility: {season_stats['min_visibility']:.1f}km\n")
                
                f.write("  Weather Conditions:\n")
                for condition, count in season_stats['weather_conditions'].items():
                    f.write(f"    - {condition}: {count} occurrences\n")
            
            # Location Analysis
            f.write("\n\nLOCATION-WISE ANALYSIS\n")
            f.write("-" * 30 + "\n")
            for point, point_stats in stats.get('points', {}).items():
                f.write(f"\n{point}:\n")
                f.write(f"  Average Temperature: {point_stats['avg_temperature']:.1f}°C\n")
                f.write(f"  Temperature Range: {point_stats['temperature_range'][0]:.1f}°C - {point_stats['temperature_range'][1]:.1f}°C\n")
                f.write(f"  Average Humidity: {point_stats['avg_humidity']:.1f}%\n")
                f.write(f"  Total Precipitation: {point_stats['total_precipitation']:.2f}mm\n")
            
            # Risk Assessment
            f.write("\n\nRISK ASSESSMENT\n")
            f.write("-" * 30 + "\n")
            high_risk = df[df['risk_score'] >= 7]
            if not high_risk.empty:
                f.write("High Risk Conditions Found:\n")
                for _, row in high_risk.iterrows():
                    f.write(f"  - {row['season']} at {row['point_name']}: Risk Score {row['risk_score']}\n")
                    f.write(f"    Conditions: {row['weather_condition']}, Road: {row['road_surface']}\n")
            else:
                f.write("No high-risk conditions detected in the historical data.\n")
            
            # Recommendations
            f.write("\n\nRECOMMENDATIONS\n")
            f.write("-" * 30 + "\n")
            
            # Check for monsoon risks
            monsoon_data = df[df['season'] == 'monsoon']
            if not monsoon_data.empty and monsoon_data['precipitation'].mean() > 0.5:
                f.write("- High precipitation during monsoon season - ensure proper drainage and road maintenance\n")
            
            # Check for visibility issues
            low_vis = df[df['visibility'] < 5]
            if not low_vis.empty:
                f.write("- Low visibility conditions detected - install proper lighting and warning systems\n")
            
            # Check for extreme temperatures
            if stats['overall']['avg_temperature'] > 35:
                f.write("- High average temperatures - consider heat-resistant road materials\n")
            elif stats['overall']['avg_temperature'] < 10:
                f.write("- Low average temperatures - monitor for ice formation risks\n")

def main():
    # Configuration
    API_URL = "http://43.250.40.133:6000/api/weather/route/seasonal"
    API_KEY = "h4DSeoxB88OwRw7rh42sWJlx8BphPHCi"
    CSV_FILE = "route_coordinates.csv"  # Your CSV file name
    
    # Initialize analyzer
    analyzer = RouteWeatherAnalyzer(API_URL, API_KEY)
    
    # Option 1: Load route from CSV
    # route_points = analyzer.load_route_from_csv(CSV_FILE)
    
    # Option 2: Use the sample coordinates from your data
    route_points = [
        {"latitude": 25.6824, "longitude": 88.0713, "name": "Point A"},
        {"latitude": 26.2175, "longitude": 88.1329, "name": "Point B"},
        {"latitude": 21.3884, "longitude": 81.6700, "name": "Point C"}
    ]
    
    print("Fetching seasonal weather data for route...")
    raw_data = analyzer.fetch_seasonal_weather(route_points)
    
    if raw_data:
        print("Parsing weather data...")
        df = analyzer.parse_weather_data(raw_data)
        
        if not df.empty:
            print(f"Successfully parsed {len(df)} weather records")
            
            # Calculate statistics
            print("Calculating route statistics...")
            stats = analyzer.calculate_route_statistics(df)
            
            # Generate visualizations
            print("Creating visualizations...")
            analyzer.visualize_weather_patterns(df)
            
            # Generate report
            print("Generating analysis report...")
            analyzer.generate_report(df, stats)
            
            # Save processed data
            df.to_csv('route_weather_data.csv', index=False)
            print("\nAnalysis complete! Check the following files:")
            print("- route_weather_data.csv (processed data)")
            print("- weather_report.txt (analysis report)")
            print("- weather_analysis_*.png (visualizations)")
        else:
            print("No weather data could be parsed")
    else:
        print("Failed to fetch weather data from API")

if __name__ == "__main__":
    main()