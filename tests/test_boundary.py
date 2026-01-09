"""Tests for Leeds boundary polygon validation."""
import pytest
from shapely.geometry import Point


class TestLeedsBoundary:
    """Verify Leeds boundary polygon is valid and correctly classifies locations."""
    
    def test_leeds_boundary_valid(self, leeds_boundary):
        """Leeds boundary polygon should be valid geometry."""
        assert leeds_boundary is not None, "Failed to fetch Leeds boundary"
    
    def test_leeds_city_centre_inside(self, leeds_boundary):
        """Leeds City Centre coordinates should be inside the boundary."""
        # Leeds City Centre: 53.7997, -1.5492
        city_centre = Point(-1.5492, 53.7997)
        
        assert leeds_boundary.contains(city_centre), \
            "Leeds City Centre should be inside Leeds boundary"
    
    def test_leeds_headingley_inside(self, leeds_boundary):
        """Headingley (Leeds suburb) should be inside the boundary."""
        # Headingley: 53.8194, -1.5761
        headingley = Point(-1.5761, 53.8194)
        
        assert leeds_boundary.contains(headingley), \
            "Headingley should be inside Leeds boundary"
    
    def test_leeds_roundhay_inside(self, leeds_boundary):
        """Roundhay Park (Leeds) should be inside the boundary."""
        # Roundhay: 53.8383, -1.5003
        roundhay = Point(-1.5003, 53.8383)
        
        assert leeds_boundary.contains(roundhay), \
            "Roundhay should be inside Leeds boundary"
    
    def test_bradford_outside(self, leeds_boundary):
        """Bradford city centre should be outside Leeds boundary."""
        # Bradford: 53.795, -1.75
        bradford = Point(-1.75, 53.795)
        
        assert not leeds_boundary.contains(bradford), \
            "Bradford should be outside Leeds boundary"
    
    def test_wakefield_outside(self, leeds_boundary):
        """Wakefield should be outside Leeds boundary."""
        # Wakefield: 53.683, -1.499
        wakefield = Point(-1.499, 53.683)
        
        assert not leeds_boundary.contains(wakefield), \
            "Wakefield should be outside Leeds boundary"
    
    def test_york_outside(self, leeds_boundary):
        """York should be outside Leeds boundary."""
        # York: 53.96, -1.08
        york = Point(-1.08, 53.96)
        
        assert not leeds_boundary.contains(york), \
            "York should be outside Leeds boundary"
