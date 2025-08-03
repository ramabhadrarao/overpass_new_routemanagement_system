# test_downloader.py
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set your API key
os.environ['GOOGLE_MAPS_API_KEY'] = 'YOUR_API_KEY_HERE'

from google_maps_image_downloader import GoogleMapsImageDownloader

# Create downloader
downloader = GoogleMapsImageDownloader(
    api_key=os.getenv('GOOGLE_MAPS_API_KEY'),
    tileserver_url="http://69.62.73.201:8080",
    use_tileserver=True
)

# Test data
test_turn = {
    '_id': 'test123',
    'latitude': 19.0760,
    'longitude': 72.8777,
    'riskScore': 8,
    'turnAngle': 95
}

# Download images
print("Testing image download...")
images = downloader.download_turn_images(test_turn, 'test_route', force_download=True)
print(f"Downloaded images: {images}")

# Check cache
stats = downloader.get_cache_stats('test_route')
print(f"Cache statistics: {stats}")