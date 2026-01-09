import pandas as pd
import requests
import time
import os
from shapely.geometry import shape, Point
from shapely.prepared import prep
import json

def filter_leeds_locations():
    file_path = "data/processed/leeds_street_combined.csv"
    
    print(f"Loading {file_path}...")
    df = pd.read_csv(file_path, low_memory=False)
    
    print("Fetching Leeds District boundary from OpenStreetMap...")
    headers = {'User-Agent': 'LeedsCrimeAnalysis/1.0 (internal research tool)'}
    url = "https://nominatim.openstreetmap.org/search?q=Leeds,+West+Yorkshire,+United+Kingdom&polygon_geojson=1&format=json"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        leeds_poly = None
        for item in data:
            if item.get('geojson') and item.get('type') == 'administrative':
                leeds_poly = shape(item['geojson'])
                break
        
        if not leeds_poly and data:
            leeds_poly = shape(data[0]['geojson'])
            
        if not leeds_poly:
            raise Exception("No GeoJSON found in OSM response")
            
        print("Leeds boundary fetched successfully.")
        
    except Exception as e:
        print(f"Error fetching boundary: {e}")
        return

    prepared_poly = prep(leeds_poly)

    target_mask = df['LSOA name'].isin(["Leeds (Unspecified)", "Leeds (Imputed from Grid)"])
    items_to_check = df[target_mask]
    
    if len(items_to_check) == 0:
        print("No records found needing verification.")
        return

    print(f"Found {len(items_to_check)} records to verify.")
    
    unique_coords = items_to_check[['Latitude', 'Longitude']].drop_duplicates()
    print(f"Unique locations to verify: {len(unique_coords)}")
    
    print("Performing local point-in-polygon check...")
    start_time = time.time()
    
    results = {}
    valid_count = 0
    
    for idx, row in unique_coords.iterrows():
        lat = row['Latitude']
        lon = row['Longitude']
        
        if pd.isna(lat) or pd.isna(lon):
            results[(lat, lon)] = False
            continue
            
        point = Point(lon, lat)
        is_inside = prepared_poly.contains(point)
        results[(lat, lon)] = is_inside
        if is_inside:
            valid_count += 1
            
    end_time = time.time()
    print(f"Verification complete in {end_time - start_time:.2f} seconds.")
    print(f"Valid Leeds locations: {valid_count} ({valid_count/len(unique_coords)*100:.1f}%)")
    
    target_indices = df[target_mask].index
    
    indices_to_drop = []
    indices_to_update = []
    
    for idx in target_indices:
        lat = df.at[idx, 'Latitude']
        lon = df.at[idx, 'Longitude']
        is_leeds = results.get((lat, lon), False)
        
        if not is_leeds:
            indices_to_drop.append(idx)
        else:
            indices_to_update.append(idx)
            
    print(f"Dropping {len(indices_to_drop)} non-Leeds records...")
    df_clean = df.drop(indices_to_drop)
    
    print(f"Updating {len(indices_to_update)} verified records...")
    df_clean.loc[indices_to_update, 'LSOA name'] = 'Leeds (Verified)'
    
    initial_count = len(df)
    final_count = len(df_clean)
    print(f"Initial records: {initial_count}")
    print(f"Final records: {final_count}")
    print(f"Removed: {initial_count - final_count}")

    df_clean.to_csv(file_path, index=False)
    print(f"Saved cleaned data to {file_path}")

if __name__ == "__main__":
    filter_leeds_locations()
