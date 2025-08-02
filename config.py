# config.py
# Updated configuration with optimizations
# Path: /config.py

import os
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Flask
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key')
    
    # MongoDB
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/route_risk_management')
    
    # Overpass API
    OVERPASS_API_URL = os.getenv('OVERPASS_API_URL', 'http://43.250.40.133:8080/api/interpreter')
    
    # File Upload
    UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'static/uploads')
    ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
    MAX_CONTENT_LENGTH = int(os.getenv('MAX_CONTENT_LENGTH', 16 * 1024 * 1024))
    
    # PDF
    PDF_OUTPUT_FOLDER = os.getenv('PDF_OUTPUT_FOLDER', 'static/pdfs')
    
    # Session
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # Celery
    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    
    # Default Admin
    DEFAULT_ADMIN_USERNAME = os.getenv('DEFAULT_ADMIN_USERNAME', 'admin')
    DEFAULT_ADMIN_PASSWORD = os.getenv('DEFAULT_ADMIN_PASSWORD', 'admin123')
    
    # Processing Settings
    FAST_PROCESSING = os.getenv('FAST_PROCESSING', 'true').lower() == 'true'
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5))
    CONCURRENT_WORKERS = int(os.getenv('CONCURRENT_WORKERS', 5))
    
    # Cache Settings
    ENABLE_API_CACHE = os.getenv('ENABLE_API_CACHE', 'true').lower() == 'true'
    CACHE_EXPIRY_HOURS = int(os.getenv('CACHE_EXPIRY_HOURS', 24))
    
    # API Rate Limits (requests per second)
    API_RATE_LIMITS = {
        'visualcrossing': float(os.getenv('VISUALCROSSING_RATE_LIMIT', '0.5')),
        'tomtom': float(os.getenv('TOMTOM_RATE_LIMIT', '2.0')),
        'here': float(os.getenv('HERE_RATE_LIMIT', '2.0')),
        'overpass': float(os.getenv('OVERPASS_RATE_LIMIT', '1.0')),
        'mapbox': float(os.getenv('MAPBOX_RATE_LIMIT', '2.0'))
    }
    
    # Sampling Settings for Large Routes
    MAX_WEATHER_SAMPLES = int(os.getenv('MAX_WEATHER_SAMPLES', 3))
    MAX_TRAFFIC_SAMPLES = int(os.getenv('MAX_TRAFFIC_SAMPLES', 10))
    MAX_ROAD_CONDITION_SAMPLES = int(os.getenv('MAX_ROAD_CONDITION_SAMPLES', 10))
    MAX_NETWORK_SAMPLES = int(os.getenv('MAX_NETWORK_SAMPLES', 15))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_API_CALLS = os.getenv('LOG_API_CALLS', 'true').lower() == 'true'
    
    # Performance Settings
    MONGODB_POOL_SIZE = int(os.getenv('MONGODB_POOL_SIZE', 100))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 30))
    
    # API Keys (loaded from .env)
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    VISUALCROSSING_API_KEY = os.getenv('VISUALCROSSING_API_KEY')
    TOMTOM_API_KEY = os.getenv('TOMTOM_API_KEY')
    HERE_API_KEY = os.getenv('HERE_API_KEY')
    MAPBOX_API_KEY = os.getenv('MAPBOX_API_KEY')
    TOMORROW_IO_API_KEY = os.getenv('TOMORROW_IO_API_KEY')