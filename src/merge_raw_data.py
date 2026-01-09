import pandas as pd
import glob
import os
import ast

def merge_raw_data():
    processed_file = "data/processed/leeds_street_combined.csv"
    raw_dir = "data/raw"
    
    print(f"Loading existing processed data from {processed_file}...")
    if os.path.exists(processed_file):
        df_processed = pd.read_csv(processed_file)
    else:
        print("Processed file not found. Starting fresh.")
        df_processed = pd.DataFrame(columns=[
            'Crime ID', 'Month', 'Reported by', 'Falls within', 
            'Longitude', 'Latitude', 'Location', 'LSOA code', 
            'LSOA name', 'Crime type', 'Last outcome category', 'Context'
        ])

    print(f"Loading raw data from {raw_dir}...")
    raw_files = glob.glob(os.path.join(raw_dir, "*.csv"))
    
    if not raw_files:
        print("No raw files found.")
        return

    raw_dfs = []
    for f in raw_files:
        try:
            df = pd.read_csv(f)
            raw_dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not raw_dfs:
        print("No valid raw data found.")
        return
        
    df_raw = pd.concat(raw_dfs, ignore_index=True)
    print(f"Loaded {len(df_raw)} raw records.")

    print("Normalizing raw data...")
    
    def get_lat_lon_loc(loc_str):
        try:
            if pd.isna(loc_str): return None, None, None
            d = ast.literal_eval(loc_str)
            lat = d.get('latitude')
            lon = d.get('longitude')
            street = d.get('street', {}).get('name')
            return lat, lon, street
        except:
            return None, None, None

    def get_outcome(outcome_str):
        try:
            if pd.isna(outcome_str): return ""
            d = ast.literal_eval(outcome_str)
            return d.get('category', "")
        except:
            return ""

    loc_data = df_raw['location'].apply(get_lat_lon_loc)
    df_raw['Latitude'] = [x[0] for x in loc_data]
    df_raw['Longitude'] = [x[1] for x in loc_data]
    df_raw['Location'] = [x[2] for x in loc_data]
    
    df_raw['Last outcome category'] = df_raw['outcome_status'].apply(get_outcome)
    
    df_raw['Crime ID'] = df_raw['persistent_id'].combine_first(df_raw['id'])
    df_raw['Month'] = df_raw['month']
    df_raw['Reported by'] = "West Yorkshire Police"
    df_raw['Falls within'] = "West Yorkshire Police"
    df_raw['LSOA code'] = ""
    df_raw['LSOA name'] = "Leeds (Unspecified)"
    df_raw['Context'] = df_raw['context']

    category_map = {
        'anti-social-behaviour': 'Anti-social behaviour',
        'burglary': 'Burglary',
        'criminal-damage-arson': 'Criminal damage and arson',
        'drugs': 'Drugs',
        'other-theft': 'Other theft',
        'possession-of-weapons': 'Possession of weapons',
        'public-order': 'Public order',
        'robbery': 'Robbery',
        'shoplifting': 'Shoplifting',
        'theft-from-the-person': 'Theft from the person',
        'vehicle-crime': 'Vehicle crime',
        'violent-crime': 'Violence and sexual offences',
        'bicycle-theft': 'Bicycle theft',
        'other-crime': 'Other crime'
    }
    df_raw['Crime type'] = df_raw['category'].map(category_map).fillna(df_raw['category'])

    cols = ['Crime ID', 'Month', 'Reported by', 'Falls within', 
            'Longitude', 'Latitude', 'Location', 'LSOA code', 
            'LSOA name', 'Crime type', 'Last outcome category', 'Context']
    
    df_normalized = df_raw[cols]

    print("Merging datasets...")
    
    df_combined = pd.concat([df_processed, df_normalized], ignore_index=True)
    
    initial_len = len(df_combined)
    df_combined = df_combined.drop_duplicates(subset=['Crime ID'], keep='last')
    
    print(f"Total records after merge: {len(df_combined)} (Dropped {initial_len - len(df_combined)} duplicates)")
    
    df_combined = df_combined.sort_values(by=['Month'])
    
    df_combined.to_csv(processed_file, index=False)
    print(f"Saved merged data to {processed_file}")

if __name__ == "__main__":
    merge_raw_data()
