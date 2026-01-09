import pandas as pd
import requests
import os
import json
import time
from shapely.geometry import shape, Point
from shapely.prepared import prep
from tqdm import tqdm

def assign_lsoa():
    file_path = "data/processed/leeds_street_combined.csv"
    lsoa_geojson_path = "data/raw/leeds_lsoa_2011.geojson"
    
    print(f"Loading {file_path}...")
    df = pd.read_csv(file_path, low_memory=False)
    
    if not os.path.exists(lsoa_geojson_path):
        print("Fetching Leeds LSOA 2011 boundaries from ONS API...")
        url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA_Dec_2011_Boundaries_Generalised_Clipped_BGC_EW_V3/FeatureServer/0/query?where=LSOA11NM%20like%20%27Leeds%25%27&outFields=*&f=geojson"
        
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            with open(lsoa_geojson_path, 'wb') as f:
                f.write(resp.content)
            print("LSOA boundaries saved.")
        except Exception as e:
            print(f"Failed to download LSOA boundaries: {e}")
            return
    else:
        print("Using existing LSOA boundaries.")

    with open(lsoa_geojson_path, 'r') as f:
        geojson = json.load(f)
        
    print(f"Loaded {len(geojson['features'])} LSOA polygons.")
    
    lsoa_polys = []
    for feature in geojson['features']:
        props = feature['properties']
        code = props['LSOA11CD']
        name = props['LSOA11NM']
        poly = shape(feature['geometry'])
        prepared = prep(poly)
        lsoa_polys.append({
            'code': code,
            'name': name,
            'poly': prepared
        })

    target_mask = df['LSOA name'] == "Leeds (Verified)"
    target_indices = df[target_mask].index
    
    if len(target_indices) == 0:
        print("No 'Leeds (Verified)' records found to assign.")
        return
        
    print(f"Assigning LSOAs to {len(target_indices)} records...")
    
    unique_coords = df.loc[target_indices, ['Latitude', 'Longitude']].drop_duplicates()
    print(f"Unique locations: {len(unique_coords)}")
    
    coord_map = {}
    unmatched_count = 0
    
    for idx, row in tqdm(unique_coords.iterrows(), total=len(unique_coords)):
        lat = row['Latitude']
        lon = row['Longitude']
        point = Point(lon, lat)
        
        found = False
        for lsoa in lsoa_polys:
            if lsoa['poly'].contains(point):
                coord_map[(lat, lon)] = (lsoa['code'], lsoa['name'])
                found = True
                break
        
        if not found:
            unmatched_count += 1
            coord_map[(lat, lon)] = ("E01000000", "Leeds (Unmatched)")
            
    print(f"Assignment complete. Unmatched: {unmatched_count}")
    
    print("Updating dataframe...")
    
    codes_map = {}
    names_map = {}
    
    for (lat, lon), (new_code, new_name) in coord_map.items():
        codes_map[(lat, lon)] = new_code
        names_map[(lat, lon)] = new_name

    target_lats = df.loc[target_indices, 'Latitude']
    target_lons = df.loc[target_indices, 'Longitude']
    
    new_codes_list = []
    new_names_list = []
    
    for lat, lon in zip(target_lats, target_lons):
        val = coord_map.get((lat, lon))
        if val:
            new_codes_list.append(val[0])
            new_names_list.append(val[1])
        else:
            new_codes_list.append("E01000000")
            new_names_list.append("Leeds (Unmatched)")

    df.loc[target_indices, 'LSOA code'] = new_codes_list
    df.loc[target_indices, 'LSOA name'] = new_names_list
    
    temp_path = file_path + ".tmp"
    try:
        df.to_csv(temp_path, index=False)
        if os.path.exists(file_path):
            os.remove(file_path)
        os.rename(temp_path, file_path)
        print(f"Saved updated data to {file_path}")
    except Exception as e:
        print(f"Error saving file: {e}")
        print(f"Data saved to {temp_path} instead.")

if __name__ == "__main__":
    assign_lsoa()
