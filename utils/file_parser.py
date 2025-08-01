import os
import pandas as pd
from typing import List, Dict, Optional

class FileParser:
    def parse_route_csv(self, csv_path: str) -> List[Dict]:
        """Parse route CSV file"""
        try:
            df = pd.read_csv(csv_path)
            routes = []
            
            for _, row in df.iterrows():
                route = {
                    'BU Code': str(row.get('BU Code', '')),
                    'Row Labels': str(row.get('Row Labels', '')),
                    'Customer Name': str(row.get('Customer Name', '')),
                    'Location': str(row.get('Location', ''))
                }
                routes.append(route)
                
            return routes
            
        except Exception as e:
            raise ValueError(f"Error parsing CSV file: {str(e)}")
    
    def find_coordinate_file(self, bu_code: str, row_label: str, 
                           route_data_folder: str) -> Optional[str]:
        """Find matching coordinate Excel file"""
        possible_filenames = [
            f"{bu_code}_{row_label}.xlsx",
            f"{bu_code}_00{row_label}.xlsx",
            f"{bu_code}_0{row_label}.xlsx",
            f"{bu_code}_{row_label.zfill(10)}.xlsx",
            f"{row_label}.xlsx"
        ]
        
        for filename in possible_filenames:
            filepath = os.path.join(route_data_folder, filename)
            if os.path.exists(filepath):
                return filepath
                
        return None
    
    def parse_coordinate_file(self, excel_path: str) -> List[Dict]:
        """Parse coordinate Excel file"""
        try:
            df = pd.read_excel(excel_path)
            coordinates = []
            
            # Try different column name variations
            lat_columns = ['Latitude', 'latitude', 'LAT', 'lat']
            lng_columns = ['Longitude', 'longitude', 'LON', 'lng', 'lon']
            step_columns = ['Step_ID', 'step_id', 'Step ID', 'StepID']
            
            lat_col = None
            lng_col = None
            step_col = None
            
            # Find the correct column names
            for col in lat_columns:
                if col in df.columns:
                    lat_col = col
                    break
                    
            for col in lng_columns:
                if col in df.columns:
                    lng_col = col
                    break
                    
            for col in step_columns:
                if col in df.columns:
                    step_col = col
                    break
                    
            if not lat_col or not lng_col:
                raise ValueError("Latitude or Longitude columns not found")
                
            # Extract coordinates
            for idx, row in df.iterrows():
                lat = float(row[lat_col])
                lng = float(row[lng_col])
                
                if pd.notna(lat) and pd.notna(lng):
                    coord = {
                        'latitude': lat,
                        'longitude': lng,
                        'step_id': row[step_col] if step_col else idx + 1
                    }
                    coordinates.append(coord)
                    
            # Sort by step_id if available
            if step_col:
                coordinates.sort(key=lambda x: int(x['step_id']))
                
            return coordinates
            
        except Exception as e:
            raise ValueError(f"Error parsing coordinate file: {str(e)}")