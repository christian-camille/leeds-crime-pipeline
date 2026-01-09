import pandas as pd
import os
import glob

def combine_leeds_data():
    base_dir = "data/archive"
    output_dir = "data/processed"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    start_date = "2018-01"
    end_date = "2022-10"
    
    dates = pd.date_range(start=start_date, end=end_date, freq='MS').strftime("%Y-%m").tolist()
    
    MIN_LAT = 53.69
    MAX_LAT = 53.96
    MIN_LON = -1.80
    MAX_LON = -1.29
    
    street_dfs = []
    outcomes_dfs = []
    stop_search_dfs = []
    
    print(f"Processing data from {start_date} to {end_date}...")
    
    for date in dates:
        month_dir = os.path.join(base_dir, date)
        
        if not os.path.exists(month_dir):
            print(f"Warning: Directory {month_dir} does not exist.")
            continue
            
        print(f"Processing {date}...")
        
        street_file = os.path.join(month_dir, f"{date}-west-yorkshire-street.csv")
        if os.path.exists(street_file):
            try:
                df = pd.read_csv(street_file)
                if 'LSOA name' in df.columns:
                    leeds_df = df[df['LSOA name'].str.contains('Leeds', case=False, na=False)]
                    street_dfs.append(leeds_df)
            except Exception as e:
                print(f"Error reading {street_file}: {e}")

        outcomes_file = os.path.join(month_dir, f"{date}-west-yorkshire-outcomes.csv")
        if os.path.exists(outcomes_file):
            try:
                df = pd.read_csv(outcomes_file)
                if 'LSOA name' in df.columns:
                    leeds_df = df[df['LSOA name'].str.contains('Leeds', case=False, na=False)]
                    outcomes_dfs.append(leeds_df)
            except Exception as e:
                print(f"Error reading {outcomes_file}: {e}")
                
        stop_search_file = os.path.join(month_dir, f"{date}-west-yorkshire-stop-and-search.csv")
        if os.path.exists(stop_search_file):
            try:
                df = pd.read_csv(stop_search_file)
                if 'Latitude' in df.columns and 'Longitude' in df.columns:
                    df = df.dropna(subset=['Latitude', 'Longitude'])
                    
                    mask = (
                        (df['Latitude'] >= MIN_LAT) & 
                        (df['Latitude'] <= MAX_LAT) & 
                        (df['Longitude'] >= MIN_LON) & 
                        (df['Longitude'] <= MAX_LON)
                    )
                    leeds_df = df[mask]
                    stop_search_dfs.append(leeds_df)
            except Exception as e:
                 print(f"Error reading {stop_search_file}: {e}")

    print("Combining and saving files...")
    
    if street_dfs:
        combined_street = pd.concat(street_dfs, ignore_index=True)
        output_path = os.path.join(output_dir, "leeds_street_archive.csv")
        combined_street.to_csv(output_path, index=False)
        print(f"Saved {len(combined_street)} street records to {output_path}")
    else:
        print("No street data found.")

    if outcomes_dfs:
        combined_outcomes = pd.concat(outcomes_dfs, ignore_index=True)
        output_path = os.path.join(output_dir, "leeds_outcomes_combined.csv")
        combined_outcomes.to_csv(output_path, index=False)
        print(f"Saved {len(combined_outcomes)} outcome records to {output_path}")
    else:
        print("No outcome data found.")

    if stop_search_dfs:
        combined_stop_search = pd.concat(stop_search_dfs, ignore_index=True)
        output_path = os.path.join(output_dir, "leeds_stop_and_search_combined.csv")
        combined_stop_search.to_csv(output_path, index=False)
        print(f"Saved {len(combined_stop_search)} stop and search records to {output_path}")
    else:
         print("No stop and search data found.")

if __name__ == "__main__":
    combine_leeds_data()
