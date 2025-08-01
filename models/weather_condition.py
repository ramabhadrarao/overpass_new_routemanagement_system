from datetime import datetime
from bson import ObjectId

class WeatherCondition:
    collection_name = 'weatherconditions'  # PDF generator expects this name
    
    def __init__(self, db):
        self.collection = db[self.collection_name]
        self.collection.create_index([('routeId', 1)])
        self.collection.create_index([('season', 1)])
        self.collection.create_index([('distanceFromStartKm', 1)])
        
    def create_weather_condition(self, route_id, weather_data):
        """Create weather condition record with all fields expected by PDF generator"""
        weather_doc = {
            'routeId': ObjectId(route_id),
            'latitude': weather_data['latitude'],
            'longitude': weather_data['longitude'],
            'distanceFromStartKm': weather_data['distance_from_start_km'],
            'distanceFromEndKm': weather_data.get('distance_from_end_km', 0),
            
            # Basic Weather Data
            'season': weather_data['season'],  # spring, summer, monsoon, winter
            'weatherCondition': weather_data['weather_condition'],  # clear, rainy, foggy, icy, stormy
            
            # Temperature and Conditions
            'averageTemperature': weather_data.get('average_temperature', 25),
            'humidity': weather_data.get('humidity', 60),
            'pressure': weather_data.get('pressure', 1013),
            
            # Precipitation and Wind
            'precipitationMm': weather_data.get('precipitation_mm', 0),
            'windSpeedKmph': weather_data.get('wind_speed_kmph', 10),
            'windDirection': weather_data.get('wind_direction', 'N'),
            
            # Visibility and Surface
            'visibilityKm': weather_data.get('visibility_km', 10),
            'roadSurfaceCondition': weather_data.get('road_surface_condition', 'dry'),
            
            # Risk Assessment
            'riskScore': weather_data['risk_score'],
            'monsoonRisk': weather_data.get('monsoon_risk', 5),
            
            # Impact Assessment
            'drivingConditionImpact': weather_data.get('driving_condition_impact', 'minimal'),
            'recommendedPrecautions': weather_data.get('recommended_precautions', []),
            
            # Enhanced Weather Data
            'uvIndex': weather_data.get('uv_index', 5),
            'extremeWeatherHistory': weather_data.get('extreme_weather_history', []),
            
            # Data Quality
            'dataSource': weather_data.get('data_source', 'WEATHER_ANALYSIS'),
            'forecastAccuracy': weather_data.get('forecast_accuracy', 80),
            'dataYear': weather_data.get('data_year', datetime.now().year),
            
            # Timestamps
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        return self.collection.insert_one(weather_doc)
    
    def get_route_weather_conditions(self, route_id):
        """Get all weather conditions for a route"""
        return list(self.collection.find({'routeId': ObjectId(route_id)})
                   .sort('distanceFromStartKm', 1))
    
    def get_seasonal_conditions(self, route_id, season):
        """Get weather conditions for a specific season"""
        return list(self.collection.find({
            'routeId': ObjectId(route_id),
            'season': season
        }).sort('distanceFromStartKm', 1))