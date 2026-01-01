# Testing Guide for Betafits CRM

## Overview
This document describes how to test your work in the Betafits CRM repository.

## Setup

### 1. Install Development Dependencies
```bash
pip install -e ".[dev]"
```

This installs all testing tools including:
- **pytest** - Testing framework
- **pytest-cov** - Coverage reporting
- **pytest-asyncio** - Async test support
- **pytest-mock** - Mocking utilities
- **black** - Code formatting
- **isort** - Import sorting
- **mypy** - Type checking
- **flake8** - Code linting
- **bandit** - Security checks

## Running Tests

### Basic Test Commands

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/unit/test_email.py

# Run specific test function
pytest tests/unit/test_email.py::test_email_validation

# Run specific test class
pytest tests/unit/test_company.py::TestCompanyProcessing

# Run tests by marker
pytest -m unit        # Only unit tests
pytest -m integration # Only integration tests
pytest -m email       # Only email-related tests
```

### Coverage Reports

```bash
# Run tests with coverage
pytest --cov=lib --cov=services --cov=apps --cov=workflows

# Generate HTML coverage report
pytest --cov=lib --cov=services --cov-report=html

# View coverage report (opens htmlcov/index.html in browser)
```

### Watch Mode (Run tests on file change)

```bash
# Install pytest-watch
pip install pytest-watch

# Run tests in watch mode
ptw
```

## Test Organization

```
tests/
├── __init__.py
├── conftest.py           # Shared fixtures and configuration
├── unit/                 # Unit tests (fast, isolated)
│   ├── test_email.py
│   ├── test_company.py
│   └── test_utils.py
└── integration/          # Integration tests (may need external services)
    ├── test_api.py
    └── test_database.py
```

## Writing Tests

### Example Unit Test

```python
def test_email_validation():
    """Test email validation logic."""
    valid_email = "user@example.com"
    assert "@" in valid_email
    assert "." in valid_email


class TestEmailProcessing:
    """Test suite for email processing functions."""
    
    def test_parse_email_metadata(self, sample_email_data):
        """Test email metadata parsing."""
        assert sample_email_data["message_id"] is not None
        assert len(sample_email_data["from_email"]) > 0
```

### Using Fixtures

Fixtures are defined in `tests/conftest.py` and can be used in any test:

```python
def test_my_function(sample_email_data, sample_company_data):
    """Test using fixtures."""
    # Use the fixtures
    assert sample_email_data["subject"] == "Test Email"
    assert sample_company_data["name"] == "Acme Corporation"
```

### Async Tests

```python
@pytest.mark.asyncio
async def test_async_function():
    """Test async functionality."""
    result = await some_async_function()
    assert result is not None
```

### Mocking

```python
def test_with_mock(mocker):
    """Test with mocked dependencies."""
    mock_api = mocker.patch('services.api.call_external_api')
    mock_api.return_value = {"status": "success"}
    
    result = my_function_that_calls_api()
    assert result["status"] == "success"
    mock_api.assert_called_once()
```

## Test Markers

Use markers to categorize and selectively run tests:

```python
@pytest.mark.unit
def test_fast_unit():
    """Fast unit test."""
    pass


@pytest.mark.integration
def test_needs_database():
    """Integration test requiring database."""
    pass


@pytest.mark.slow
def test_long_running():
    """Slow test."""
    pass


@pytest.mark.skip(reason="Requires running server")
def test_api_endpoint():
    """Test that requires external server."""
    pass
```

Run specific markers:
```bash
pytest -m unit
pytest -m "not slow"
pytest -m "integration and email"
```

## Code Quality Checks

### Format Code
```bash
# Format with black
black .

# Sort imports
isort .
```

### Lint Code
```bash
# Run flake8
flake8 lib/ services/ apps/ workflows/

# Type check with mypy
mypy lib/ services/ apps/
```

### Security Check
```bash
# Run bandit security checks
bandit -r lib/ services/ apps/
```

### Run All Quality Checks
```bash
# Format, lint, type check, and test
black . && isort . && flake8 . && mypy . && pytest
```

## Continuous Testing Workflow

### Recommended Development Workflow

1. **Write a failing test** for your new feature
   ```bash
   pytest tests/unit/test_myfeature.py -v
   ```

2. **Implement the feature** until the test passes

3. **Run all tests** to ensure nothing broke
   ```bash
   pytest
   ```

4. **Check code quality**
   ```bash
   black . && isort . && flake8 .
   ```

5. **Verify coverage** (aim for >85%)
   ```bash
   pytest --cov --cov-report=term-missing
   ```

## Integration Testing

Integration tests are skipped by default. To run them:

1. **Start required services** (API server, database, etc.)

2. **Remove skip markers** or set up service fixtures

3. **Run integration tests**
   ```bash
   pytest -m integration -v
   ```

### Example: Testing API Server

```python
# Remove @pytest.mark.skip decorator
def test_health_endpoint(self):
    """Test the /health endpoint."""
    import requests
    response = requests.get("http://localhost:3030/api/v1/health")
    assert response.status_code == 200
```

## Pre-commit Hooks (Optional)

Set up pre-commit hooks to automatically check code before commits:

```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## Troubleshooting

### Tests Not Found
- Ensure test files start with `test_`
- Ensure test functions start with `test_`
- Check `pytest.ini` configuration

### Import Errors
- Ensure the project is installed in editable mode: `pip install -e .`
- Check your Python path includes the project root

### Async Test Issues
- Ensure `pytest-asyncio` is installed
- Use `@pytest.mark.asyncio` decorator
- Check `asyncio_mode = "auto"` in pytest.ini

## Tips for Effective Testing

1. **Write tests as you code** - Don't wait until the end
2. **Test one thing at a time** - Keep tests focused
3. **Use descriptive names** - Make test purpose clear
4. **Keep tests fast** - Unit tests should run in milliseconds
5. **Avoid test interdependence** - Tests should be independent
6. **Use fixtures** - Share test data and setup
7. **Mock external dependencies** - Don't rely on external services in unit tests
8. **Aim for high coverage** - But focus on meaningful tests, not just coverage numbers

## Next Steps

- Add tests for specific modules as you develop them
- Expand integration tests when services are deployed
- Set up CI/CD pipeline to run tests automatically
- Add performance benchmarks for critical paths

## Quick Reference

```bash
# Run all unit tests
pytest -m unit -v

# Run with coverage
pytest --cov --cov-report=html

# Run specific file
pytest tests/unit/test_email.py -v

# Run and stop on first failure
pytest -x

# Show print statements
pytest -s

# Run last failed tests
pytest --lf

# Run tests in parallel (install pytest-xdist)
pytest -n auto
```
