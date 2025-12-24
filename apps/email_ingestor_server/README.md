# Email Ingestor API Server

FastAPI server for the Betafits Email Ingestor Pipeline.

## Quick Start

### Install Dependencies

```bash
pip install -e ".[dev]"
```

### Run the Server

**Option 1: Using uvicorn directly**
```bash
uvicorn apps.email_ingestor_server.main:app --host 0.0.0.0 --port 3030 --reload
```

**Option 2: Using the Python module**
```bash
python -m apps.email_ingestor_server.main
```

**Option 3: Using the console script (after pip install)**
```bash
betafits-email-server
```

## API Endpoints

### `POST /api/v1/ingest`

Ingest and process an email through the pipeline.

**Request Body (JSON):**
```json
{
  "message_id": "unique-gmail-id",
  "from_email": "sender@example.com",
  "to_emails": ["recipient@betafits.com"],
  "cc_emails": [],
  "bcc_emails": [],
  "subject": "Product Inquiry",
  "body": "Email body text...",
  "received_at": "2025-11-24T10:30:00Z",
  "thread_id": "thread-id",
  "labels": ["INBOX"],
  "is_read": false,
  "attachments": []
}
```

**Response (JSON):**
```json
{
  "success": true,
  "email_id": "processed-email-id",
  "sent_status": "sent",
  "message_id": "unique-gmail-id",
  "duration_seconds": 3.45,
  "timestamp": "2025-11-24T10:30:05Z"
}
```

**Example with curl:**
```bash
curl -X POST http://localhost:3030/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d @examples/sample_email.json
```

**Example with Python requests:**
```python
import requests

email_data = {
    "message_id": "test-123",
    "from_email": "john@acme.com",
    "to_emails": ["support@betafits.com"],
    "subject": "Demo Request",
    "body": "I'd like to schedule a demo.",
    "received_at": "2025-12-08T10:00:00Z",
    "thread_id": "thread-123",
    "labels": ["INBOX"],
    "is_read": False,
    "attachments": []
}

response = requests.post(
    "http://localhost:3030/api/v1/ingest",
    json=email_data
)
print(response.json())
```

### `GET /api/v1/health`

Health check endpoint.

**Response (JSON):**
```json
{
  "status": "healthy",
  "service": "email-ingestor-pipeline",
  "version": "1.0.0",
  "timestamp": "2025-11-24T10:30:00Z",
  "graph_compiled": true
}
```

**Example:**
```bash
curl http://localhost:3030/api/v1/health
```

### `GET /`

Root endpoint with API information.

### `GET /docs`

Interactive API documentation (Swagger UI).

### `GET /redoc`

Alternative API documentation (ReDoc).

## Configuration

All configuration is loaded from environment variables via `.env` file:

```bash
# Required
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-key
OPENROUTER_API_KEY=your-key

# Optional
ENVIRONMENT=development
LOG_LEVEL=INFO
REQUEST_TIMEOUT=30
```

## Production Deployment

### Using Gunicorn with Uvicorn Workers

```bash
gunicorn apps.email_ingestor_server.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:3030 \
  --timeout 120
```

### Using Docker

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . .
RUN pip install -e .

EXPOSE 3030
CMD ["uvicorn", "apps.email_ingestor_server.main:app", "--host", "0.0.0.0", "--port", "3030"]
```

### Environment Variables for Production

```bash
ENVIRONMENT=production
LOG_LEVEL=INFO
WORKERS=4
```

## Pipeline Flow

The API processes emails through an 8-node LangGraph pipeline:

1. **Email Router** - Parse and deduplicate
2. **Classifier** - Categorize as CRM/Customer Success/Spam
3. **KG+RAG Update** - Store in knowledge graph (Graphiti extracts entities/relations)
4. **Context Retriever** - Fetch relevant context
5. **Response Drafter** - Generate AI reply
6. **JSON Formatter** - Validate CRM schema
7. **Email Sender** - Send or queue response
8. **Validator** - Audit trail

## Error Handling

The API returns appropriate HTTP status codes:

- **200 OK** - Email processed successfully
- **400 Bad Request** - Invalid email data (validation error)
- **422 Unprocessable Entity** - Pydantic validation failed
- **500 Internal Server Error** - Processing error

All responses include timestamps and error details when applicable.

## Monitoring

### Logs

All logs are written to stdout in structured format:

```
2025-11-24 10:30:00 - apps.email_ingestor_server.main - INFO - Received email ingestion request: test-123
2025-11-24 10:30:03 - apps.email_ingestor_server.main - INFO - ✓ Email test-123 processed successfully in 3.45s (status: sent)
```

### Health Checks

Use the `/api/v1/health` endpoint for:
- Kubernetes liveness/readiness probes
- Load balancer health checks
- Monitoring systems

## Development

### Run with Hot Reload

```bash
uvicorn apps.email_ingestor_server.main:app --reload --port 3030
```

### Access API Documentation

- Swagger UI: http://localhost:3030/docs
- ReDoc: http://localhost:3030/redoc

### Test the API

```bash
# Install test dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/integration/test_api_server.py -v
```
