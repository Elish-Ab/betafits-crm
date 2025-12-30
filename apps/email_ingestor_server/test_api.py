"""Test script for Email Ingestor API Server.

This script demonstrates how to use the FastAPI server endpoints.
"""

import requests
from datetime import datetime


def test_health_endpoint():
    """Test the health check endpoint."""
    print("Testing health endpoint...")
    response = requests.get("http://localhost:3030/api/v1/health")

    if response.status_code == 200:
        print("✓ Health check passed")
        print(f"  Response: {response.json()}")
    else:
        print(f"✗ Health check failed: {response.status_code}")

    return response.status_code == 200


def test_ingest_endpoint():
    """Test the email ingestion endpoint."""
    print("\nTesting email ingestion endpoint...")

    # Sample email data
    email_data = {
        "message_id": "test-email-123",
        "from_email": "john.smith@acme.com",
        "to_emails": ["support@betafits.com"],
        "cc_emails": [],
        "bcc_emails": [],
        "subject": "Demo Request - Website Redesign",
        "body": "Hi, I'm John Smith from Acme Corp. We're interested in your website redesign services. Could we schedule a demo call next week?",
        "received_at": datetime.utcnow().isoformat() + "Z",
        "thread_id": "thread-test-123",
        "labels": ["INBOX"],
        "is_read": False,
        "attachments": [],
    }

    try:
        response = requests.post(
            "http://localhost:3030/api/v1/ingest", json=email_data, timeout=60
        )

        if response.status_code == 200:
            result = response.json()
            print("✓ Email ingestion successful")
            print(f"  Success: {result['success']}")
            print(f"  Duration: {result['duration_seconds']}s")
            print(f"  Status: {result['sent_status']}")
            print(f"  Response: {result}")
        else:
            print(f"✗ Email ingestion failed: {response.status_code}")
            print(f"  Response: {response.text}")

        return response.status_code == 200

    except requests.exceptions.ConnectionError:
        print("✗ Connection error - Is the server running on http://localhost:3030?")
        return False
    except requests.exceptions.Timeout:
        print("✗ Request timed out - Pipeline processing took too long")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 70)
    print("Email Ingestor API Server - Test Suite")
    print("=" * 70)
    print("\nMake sure the server is running:")
    print("  uvicorn apps.email_ingestor_server.main:app --port 3030")
    print("=" * 70)

    # Test health endpoint
    health_ok = test_health_endpoint()

    if not health_ok:
        print("\n✗ Health check failed. Server may not be running.")
        print(
            "  Start the server with: uvicorn apps.email_ingestor_server.main:app --port 3030"
        )
        return

    # Test ingestion endpoint
    ingest_ok = test_ingest_endpoint()

    print("\n" + "=" * 70)
    print("Test Summary:")
    print(f"  Health Check: {'✓ PASS' if health_ok else '✗ FAIL'}")
    print(f"  Email Ingest: {'✓ PASS' if ingest_ok else '✗ FAIL'}")
    print("=" * 70)


if __name__ == "__main__":
    main()
