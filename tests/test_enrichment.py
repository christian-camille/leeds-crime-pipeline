"""Tests for data enrichment quality."""
import pytest


class TestEnrichmentQuality:
    """Verify that enriched data meets quality standards."""
    
    def test_ward_column_exists(self, processed_data):
        """Processed data should have Ward Name column."""
        assert 'Ward Name' in processed_data.columns, \
            "Ward Name column missing from processed data"
    
    def test_postcode_column_exists(self, processed_data):
        """Processed data should have Postcode District column."""
        assert 'Postcode District' in processed_data.columns, \
            "Postcode District column missing from processed data"
    
    def test_lsoa_columns_exist(self, processed_data):
        """Processed data should have LSOA code and name columns."""
        assert 'LSOA code' in processed_data.columns, \
            "LSOA code column missing"
        assert 'LSOA name' in processed_data.columns, \
            "LSOA name column missing"
    
    def test_unknown_ward_rate_acceptable(self, processed_data):
        """Unknown Ward rate should be below 5%."""
        total = len(processed_data)
        unknowns = len(processed_data[processed_data['Ward Name'] == 'Unknown'])
        unknown_rate = unknowns / total
        
        assert unknown_rate < 0.05, \
            f"Unknown Ward rate {unknown_rate:.1%} exceeds 5% threshold"
    
    def test_unknown_postcode_rate_acceptable(self, processed_data):
        """Unknown Postcode District rate should be below 5%."""
        total = len(processed_data)
        unknowns = len(processed_data[processed_data['Postcode District'] == 'Unknown'])
        unknown_rate = unknowns / total
        
        assert unknown_rate < 0.05, \
            f"Unknown Postcode rate {unknown_rate:.1%} exceeds 5% threshold"
    
    def test_data_has_records(self, processed_data):
        """Processed data should contain records."""
        assert len(processed_data) > 0, "Processed data is empty"
    
    def test_required_columns_present(self, processed_data):
        """All required columns should be present."""
        required = [
            'Crime ID', 'Month', 'Latitude', 'Longitude', 
            'Crime type', 'LSOA code', 'LSOA name',
            'Ward Name', 'Postcode District'
        ]
        
        missing = [col for col in required if col not in processed_data.columns]
        
        assert not missing, f"Missing required columns: {missing}"
    
    def test_coordinates_in_valid_range(self, processed_data):
        """All coordinates should be within Leeds bounding box."""
        df = processed_data.dropna(subset=['Latitude', 'Longitude'])
        
        # Leeds approximate bounds
        MIN_LAT, MAX_LAT = 53.69, 53.96
        MIN_LON, MAX_LON = -1.80, -1.29
        
        lat_valid = (df['Latitude'] >= MIN_LAT) & (df['Latitude'] <= MAX_LAT)
        lon_valid = (df['Longitude'] >= MIN_LON) & (df['Longitude'] <= MAX_LON)
        
        valid_rate = (lat_valid & lon_valid).mean()
        
        assert valid_rate > 0.95, \
            f"Only {valid_rate:.1%} of coordinates within Leeds bounds"
