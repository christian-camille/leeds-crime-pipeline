import pandas as pd
import os

def merge_datasets():
    archive_file = "data/processed/leeds_street_archive.csv"
    api_file = "data/processed/leeds_street_api_clean.csv"
    output_file = "data/processed/leeds_street_combined.csv"
    
    dfs = []
    
    if os.path.exists(archive_file):
        print(f"Loading archive: {archive_file}...")
        df_archive = pd.read_csv(archive_file, low_memory=False)
        dfs.append(df_archive)
        print(f"  Archive records: {len(df_archive)}")
    else:
        print(f"Warning: Archive file {archive_file} not found.")
        
    if os.path.exists(api_file):
        print(f"Loading processed API data: {api_file}...")
        df_api = pd.read_csv(api_file, low_memory=False)
        dfs.append(df_api)
        print(f"  API records: {len(df_api)}")
    else:
        print(f"Warning: API file {api_file} not found.")
        
    if not dfs:
        print("No data to merge.")
        return

    print("Combining datasets...")
    df_combined = pd.concat(dfs, ignore_index=True)
    initial_count = len(df_combined)
    
    print("Deduplicating based on 'Crime ID'...")
    
    mask_id = df_combined['Crime ID'].notna()
    df_ids = df_combined[mask_id]
    df_no_ids = df_combined[~mask_id]
    
    df_ids_dedup = df_ids.drop_duplicates(subset=['Crime ID'], keep='last')
    
    df_final = pd.concat([df_ids_dedup, df_no_ids], ignore_index=True)
    
    print(f"Dropped {initial_count - len(df_final)} duplicates.")
    
    print("Sorting by Month...")
    if 'Month' in df_final.columns:
        df_final = df_final.sort_values(by='Month')
        
    print(f"Saving {len(df_final)} records to {output_file}...")
    df_final.to_csv(output_file, index=False)
    print("Merge complete.")

if __name__ == "__main__":
    merge_datasets()
