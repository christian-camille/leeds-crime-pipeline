"""Tests for location validation via external API."""
import pytest
import requests
import pandas as pd


class TestLocationValidation:
    """Validate that sample locations are correctly identified as Leeds."""
    
    @pytest.mark.timeout(30)
    def test_sample_locations_in_leeds(self, processed_data):
        """Random sample of locations should be in Leeds district."""
        df = processed_data.dropna(subset=['Latitude', 'Longitude'])
        
        # Sample 10 random locations
        sample_size = min(10, len(df))
        sample = df.sample(sample_size, random_state=42)
        
        leeds_count = 0
        checked = 0
        
        for _, row in sample.iterrows():
            lat, lon = row['Latitude'], row['Longitude']
            
            try:
                url = f"https://api.postcodes.io/postcodes?lon={lon}&lat={lat}&limit=1"
                response = requests.get(url, timeout=5)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('result'):
                        district = data['result'][0].get('admin_district', '')
                        if 'Leeds' in district:
                            leeds_count += 1
                        checked += 1
            except Exception:
                continue
        
        if checked == 0:
            pytest.skip("Could not verify any locations via API")
        
        leeds_rate = leeds_count / checked
        
        assert leeds_rate >= 0.8, \
            f"Only {leeds_rate:.0%} of sampled locations confirmed as Leeds"
    
    def test_known_leeds_postcodes_present(self, processed_data):
        """Common Leeds postcode districts should be present."""
        if 'Postcode District' not in processed_data.columns:
            pytest.skip("Postcode District column not present")
        
        leeds_postcodes = ['LS1', 'LS2', 'LS6', 'LS7', 'LS8']
        present = processed_data['Postcode District'].unique()
        
        found = [pc for pc in leeds_postcodes if pc in present]
        
        assert len(found) >= 3, \
            f"Expected at least 3 Leeds postcodes, found: {found}"
    
    def test_no_invalid_postcodes(self, processed_data):
        """Non-Leeds postcodes should not be present (except Unknown)."""
        if 'Postcode District' not in processed_data.columns:
            pytest.skip("Postcode District column not present")
        
        # Non-Leeds West Yorkshire postcodes that shouldn't appear
        invalid = ['BD', 'HX', 'WF', 'HD']
        present = processed_data['Postcode District'].unique()
        
        found_invalid = [pc for pc in invalid if pc in present]
        
        assert not found_invalid, \
            f"Found non-Leeds postcodes: {found_invalid}"
