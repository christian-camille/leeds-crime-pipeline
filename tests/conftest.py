import pytest
import requests
import os
import pandas as pd
from shapely.geometry import shape
from shapely.prepared import prep

LEEDS_BOUNDARY_URL = "https://nominatim.openstreetmap.org/search?q=Leeds,+West+Yorkshire,+United+Kingdom&polygon_geojson=1&format=json"
PROCESSED_DATA_PATH = "data/processed/leeds_street_combined.csv"


@pytest.fixture(scope="session")
def leeds_boundary():
    """Fetch and cache the Leeds administrative boundary polygon."""
    headers = {'User-Agent': 'LeedsCrimeTests/1.0'}
    
    try:
        resp = requests.get(LEEDS_BOUNDARY_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        poly = None
        for item in data:
            if item.get('geojson') and item.get('type') == 'administrative':
                poly = shape(item['geojson'])
                break
        
        if not poly and data:
            poly = shape(data[0]['geojson'])
            
        return prep(poly) if poly else None
        
    except Exception:
        pytest.skip("Could not fetch Leeds boundary from OSM")


@pytest.fixture(scope="session")
def processed_data():
    """Load the processed crime dataset if it exists."""
    if not os.path.exists(PROCESSED_DATA_PATH):
        pytest.skip(f"Processed data not found at {PROCESSED_DATA_PATH}")
    
    return pd.read_csv(PROCESSED_DATA_PATH, low_memory=False)
