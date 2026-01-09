import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

def patch_enrichment():
    file_path = "data/processed/leeds_street_combined.csv"
    print(f"Loading {file_path}...")
    df = pd.read_csv(file_path, low_memory=False)
    
    mask = (df['Ward Name'] == 'Unknown') | (df['Postcode District'] == 'Unknown')
    unknown_df = df[mask]
    
    print(f"Found {len(unknown_df)} records with 'Unknown' Ward or Postcode.")
    
    if len(unknown_df) == 0:
        print("Nothing to patch.")
        return

    unique_coords = unknown_df[['Latitude', 'Longitude']].drop_duplicates().dropna()
    print(f"Unique locations to re-check: {len(unique_coords)}")
    
    coord_map = {}
    
    batch_size = 100
    records = [row for row in unique_coords.itertuples(index=False)]
    chunks = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
    
    print(f"Fetching data for {len(chunks)} batches using 10 threads (Radius=2000m)...")
    
    start_time = time.time()
    
    def fetch_batch(chunk):
        results_map = {}
        payload = {
            "geolocations": [
                {"longitude": r.Longitude, "latitude": r.Latitude, "limit": 1, "radius": 2000} 
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
                    
                    if res.get('result'):
                         item = res['result'][0]
                         ward = item.get('admin_ward') or item.get('ward') or "Unknown"
                         raw_pc = item.get('postcode')
                         if raw_pc:
                             pcd = raw_pc.split(' ')[0]
                    
                    if ward != "Unknown" or pcd != "Unknown":
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
            
    print(f"Patch lookup complete in {time.time() - start_time:.1f}s. Found {len(coord_map)} new matches.")
    
    print("Applying patches...")
    
    hits = 0
    target_lats = unknown_df['Latitude'].values
    target_lons = unknown_df['Longitude'].values
    indices = unknown_df.index
    
    update_indices = []
    update_wards = []
    update_pcds = []
    
    for idx, lat, lon in zip(indices, target_lats, target_lons):
        val = coord_map.get((lat, lon))
        if val:
            hits += 1
            update_indices.append(idx)
            update_wards.append(val['ward'])
            update_pcds.append(val['pcd'])
            
    if update_indices:
        df.loc[update_indices, 'Ward Name'] = update_wards
        df.loc[update_indices, 'Postcode District'] = update_pcds
        print(f"Patched {hits} records.")
    else:
        print("No records patched.")
        
    final_unknown = len(df[df['Ward Name'] == 'Unknown'])
    print(f"Remaining Unknown Wards: {final_unknown} ({final_unknown/len(df)*100:.2f}%)")
    
    temp_path = file_path + ".tmp"
    df.to_csv(temp_path, index=False)
    if os.path.exists(file_path):
        os.remove(file_path)
    os.rename(temp_path, file_path)
    print("Saved patched file.")

if __name__ == "__main__":
    patch_enrichment()
