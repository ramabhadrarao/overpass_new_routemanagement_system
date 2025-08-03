# test_tileserver_only.py
import os
import logging
import requests
from google_maps_image_downloader import GoogleMapsImageDownloader

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def test_tileserver_connectivity():
    """Test TileServer connectivity and available endpoints"""
    tileserver_url = "http://69.62.73.201:8080"
    print(f"\n🌐 Testing TileServer at {tileserver_url}")
    
    try:
        # Test base URL
        response = requests.get(tileserver_url, timeout=10)
        print(f"✅ TileServer accessible: HTTP {response.status_code}")
        
        # Test available endpoints
        endpoints = [
            "/styles",
            "/data",
            "/fonts",
        ]
        
        for endpoint in endpoints:
            try:
                response = requests.get(f"{tileserver_url}{endpoint}", timeout=5)
                print(f"  {endpoint}: HTTP {response.status_code}")
            except Exception as e:
                print(f"  {endpoint}: Failed - {e}")
        
        # Check available styles
        try:
            response = requests.get(f"{tileserver_url}/styles", timeout=5)
            if response.status_code == 200:
                styles = response.json()
                print(f"\n📋 Available styles: {list(styles.keys())}")
                
                # Check each style
                for style_name in styles.keys():
                    test_url = f"{tileserver_url}/styles/{style_name}/10/100/100.png"
                    response = requests.get(test_url, timeout=5)
                    print(f"  Style '{style_name}': {'✅ Working' if response.status_code == 200 else '❌ Not working'}")
        except Exception as e:
            print(f"❌ Could not get styles: {e}")
            
    except Exception as e:
        print(f"❌ TileServer connection failed: {e}")
        return False
    
    return True

def test_tileserver_download():
    """Test downloading from TileServer only"""
    print("\n📥 Testing TileServer-only download...")
    
    # Create downloader with empty API key (TileServer only)
    downloader = GoogleMapsImageDownloader(
        api_key='',  # No Google API key
        tileserver_url="http://69.62.73.201:8080",
        use_tileserver=True
    )
    
    # Test Mumbai coordinates
    test_location = {
        'latitude': 19.0760,
        'longitude': 72.8777,
        'description': 'Mumbai Test Location'
    }
    
    print(f"\n📍 Test location: {test_location['description']}")
    print(f"   Coordinates: {test_location['latitude']}, {test_location['longitude']}")
    
    # Try different zoom levels
    zoom_levels = [10, 12, 14, 16]
    
    for zoom in zoom_levels:
        print(f"\n🔍 Testing zoom level {zoom}...")
        
        # Test satellite tiles
        print("  Testing india-satellite tiles...")
        satellite_data = downloader.download_tiles_and_stitch(
            test_location['latitude'],
            test_location['longitude'],
            zoom,
            'india-satellite',
            size=(640, 480)
        )
        
        if satellite_data:
            print(f"  ✅ Satellite tiles downloaded: {len(satellite_data)} bytes")
            # Save test image
            with open(f'test_satellite_zoom{zoom}.png', 'wb') as f:
                f.write(satellite_data)
            print(f"  💾 Saved as test_satellite_zoom{zoom}.png")
        else:
            print(f"  ❌ Satellite tiles failed")
        
        # Test roadmap tiles
        print("  Testing india tiles...")
        roadmap_data = downloader.download_tiles_and_stitch(
            test_location['latitude'],
            test_location['longitude'],
            zoom,
            'india',
            size=(640, 480)
        )
        
        if roadmap_data:
            print(f"  ✅ Roadmap tiles downloaded: {len(roadmap_data)} bytes")
            # Save test image
            with open(f'test_roadmap_zoom{zoom}.png', 'wb') as f:
                f.write(roadmap_data)
            print(f"  💾 Saved as test_roadmap_zoom{zoom}.png")
        else:
            print(f"  ❌ Roadmap tiles failed")

def check_tile_coordinates():
    """Calculate and check tile coordinates for Mumbai"""
    print("\n📐 Calculating tile coordinates for Mumbai...")
    
    from google_maps_image_downloader import GoogleMapsImageDownloader
    
    # Create a dummy instance just to use the coordinate conversion
    downloader = GoogleMapsImageDownloader(api_key='', use_tileserver=False)
    
    lat, lng = 19.0760, 72.8777
    
    for zoom in range(8, 19):
        x, y = downloader.lat_lon_to_tile(lat, lng, zoom)
        print(f"  Zoom {zoom}: Tile ({x}, {y})")
        
        # Check if this tile exists
        url = f"http://69.62.73.201:8080/data/india-satellite/{zoom}/{x}/{y}.png"
        try:
            response = requests.head(url, timeout=5)
            status = "✅" if response.status_code == 200 else f"❌ {response.status_code}"
            print(f"    Satellite tile: {status}")
        except:
            print(f"    Satellite tile: ❌ Error")

def main():
    """Run all TileServer tests"""
    print("="*60)
    print("TILESERVER-ONLY TEST SUITE")
    print("="*60)
    
    # Test 1: Basic connectivity
    if not test_tileserver_connectivity():
        print("\n❌ TileServer not accessible. Please check:")
        print("  1. Is TileServer running at http://69.62.73.201:8080?")
        print("  2. Is there a network/firewall issue?")
        return
    
    # Test 2: Check tile coordinates
    check_tile_coordinates()
    
    # Test 3: Download test
    test_tileserver_download()
    
    print("\n✅ Tests completed! Check the generated PNG files.")

if __name__ == "__main__":
    main()