import pandas as pd
import requests
import os
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

def enrich_data():
    input_file = "data/processed/leeds_street_combined.csv"
    
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file, low_memory=False)
    
    unique_coords = df[['Latitude', 'Longitude']].drop_duplicates().dropna()
    print(f"Unique locations to enrich: {len(unique_coords)}")
    
    coord_map = {}
    
    batch_size = 100
    records = [row for row in unique_coords.itertuples(index=False)]
    chunks = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
    
    print(f"Fetching data for {len(chunks)} batches using 10 threads...")
    
    start_time = time.time()
    
    def fetch_batch(chunk):
        results_map = {}
        payload = {
            "geolocations": [
                {"longitude": r.Longitude, "latitude": r.Latitude, "limit": 1, "radius": 200} 
                for r in chunk
            ]
        }
        try:
            resp = requests.post("https://api.postcodes.io/postcodes", json=payload, timeout=20)
            if resp.status_code == 200:
                results = resp.json().get('result', [])
                for i, res in enumerate(results):
                    lat = chunk[i].Latitude
                    lon = chunk[i].Longitude
                    
                    ward = "Unknown"
                    pcd = "Unknown"
                    
                    if res['result']:
                         item = res['result'][0]
                         ward = item.get('admin_ward') or item.get('ward') or "Unknown"
                         raw_pc = item.get('postcode')
                         if raw_pc:
                             pcd = raw_pc.split(' ')[0]
                    
                    results_map[(lat, lon)] = {'ward': ward, 'pcd': pcd}
            return results_map
        except Exception as e:
            print(f"Error: {e}")
            return {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_batch, chunk) for chunk in chunks]
        
        for future in tqdm(as_completed(futures), total=len(chunks)):
            res = future.result()
            coord_map.update(res)
            
    print(f"Enrichment lookup complete in {time.time() - start_time:.1f}s")
    
    print("Applying mappings to main dataset...")
    
    lats = df['Latitude'].values
    lons = df['Longitude'].values
    
    wards = []
    pcds = []
    
    count_hit = 0
    count_miss = 0
    
    for lat, lon in zip(lats, lons):
        val = coord_map.get((lat, lon))
        if val:
            wards.append(val['ward'])
            pcds.append(val['pcd'])
            count_hit += 1
        else:
            wards.append("Unknown")
            pcds.append("Unknown")
            count_miss += 1
            
    df['Ward Name'] = wards
    df['Postcode District'] = pcds
    
    print(f"Applied. Hits: {count_hit}, Misses: {count_miss}")
    
    print(f"Saving to {input_file} (overwriting)...")
    temp_save = input_file + ".tmp"
    df.to_csv(temp_save, index=False)
    
    if os.path.exists(input_file):
        os.remove(input_file)
    os.rename(temp_save, input_file)
    print("Done.")

if __name__ == "__main__":
    enrich_data()
