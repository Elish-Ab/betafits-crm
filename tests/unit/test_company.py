"""Unit tests for company models and data."""
import pytest


def test_company_data_structure(sample_company_data):
    """Test that sample company data has required fields."""
    assert "name" in sample_company_data
    assert "domain" in sample_company_data
    assert sample_company_data["name"] is not None


def test_company_domain_validation(sample_company_data):
    """Test company domain validation."""
    domain = sample_company_data["domain"]
    assert "." in domain
    assert " " not in domain
    assert domain == domain.lower()


class TestCompanyProcessing:
    """Test suite for company processing functions."""
    
    def test_company_name_normalization(self, sample_company_data):
        """Test company name normalization."""
        name = sample_company_data["name"]
        assert len(name) > 0
        
    def test_company_metadata(self, sample_company_data):
        """Test company metadata fields."""
        assert "industry" in sample_company_data
        assert "size" in sample_company_data
        assert "location" in sample_company_data
