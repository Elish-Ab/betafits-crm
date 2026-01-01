"""Pytest configuration and shared fixtures."""
import pytest
from pathlib import Path
import sys

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_email_data():
    """Sample email data for testing."""
    return {
        "message_id": "test-email-123",
        "from_email": "test@example.com",
        "to_emails": ["recipient@betafits.com"],
        "cc_emails": [],
        "bcc_emails": [],
        "subject": "Test Email",
        "body": "This is a test email body.",
        "received_at": "2025-12-31T10:00:00Z",
        "thread_id": "thread-123",
        "labels": ["INBOX"],
        "is_read": False,
        "attachments": [],
    }


@pytest.fixture
def sample_company_data():
    """Sample company data for testing."""
    return {
        "name": "Acme Corporation",
        "domain": "acme.com",
        "industry": "Technology",
        "size": "50-200",
        "location": "San Francisco, CA",
    }
