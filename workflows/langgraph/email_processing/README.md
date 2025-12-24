# Email Processing Pipeline

The email processing pipeline handles the complete workflow for ingesting, classifying, enriching, and validating incoming and outgoing emails. It automatically processes emails through 4 sequential nodes.

## 📊 Pipeline Overview

```
Raw Email
    ↓
[1] Email Router (Parse & Deduplicate)
    ↓
[2] Email Classifier (CRM/Customer Success/Spam)
    ↓
[3] KG/RAG Updater (Graphiti - Entity/Relation Extraction)
    ↓
[4] Workflow Validator (Audit Trail)
    ↓
PipelineValidationLog
```

## 🔄 Nodes

### 1. Email Router
**File**: `nodes/email_router.py`

**Responsibilities**:
- Parse incoming/outgoing email data
- Validate email format and structure
- Check for duplicate emails
- Extract message and thread IDs

**Inputs**:
- `parsed_email`: ReceivedEmail or SentEmail

**Outputs**:
- `is_duplicate`: Boolean flag
- `should_skip`: Boolean flag
- `router_error`: Optional error message

**Error Handling**:
- Logs parsing errors
- Continues with reduced context if validation fails

---

### 2. Email Classifier
**File**: `nodes/email_classifier.py`

**Responsibilities**:
- Classify email into categories using LLM
- Calculate classification confidence
- Store classification result in database

**Classification Categories**:
- `crm` - Customer relationship, sales, opportunities
- `customer_success` - Support, implementation, success
- `spam` - Marketing, automated messages, irrelevant content

**Inputs**:
- `parsed_email`: Email to classify

**Outputs**:
- `labeled_email`: LabeledEmail with classification
- `should_skip`: True if classified as spam
- `classification_error`: Optional error message

**LLM Configuration**:
- Model: `openai/gpt-4o-mini` (configurable)
- Temperature: 0.3 (deterministic)
- Max tokens: 200

---

### 3. KG/RAG Updater
**File**: `nodes/kg_rag_updater.py`

**Responsibilities**:
- Extract entities (people, companies, opportunities) from email
- Extract relationships between entities
- Update Graphiti knowledge graph with `add_episode()`
- Store vector embeddings in Supabase for RAG

**Graphiti Integration**:
- Creates episodes (email conversations)
- Automatically extracts entities and relationships
- Groups by opportunity ID for context isolation

**Inputs**:
- `labeled_email`: Classified email
- `opportunity_id`: Optional, for grouping context

**Outputs**:
- `kg_nodes_created`: Number of KG nodes
- `kg_edges_created`: Number of KG edges
- `rag_vectors_upserted`: Number of vector embeddings
- `kg_update_error`: Optional error message

**Error Handling**:
- Non-blocking (logs warnings, pipeline continues)
- Partial updates allowed if some operations fail

---

### 4. Workflow Validator
**File**: `nodes/workflow_validator.py`

**Responsibilities**:
- Validate all pipeline outputs
- Generate complete audit trail
- Collect errors and warnings from all stages
- Create validation log entry

**Inputs**:
- All state variables from previous nodes

**Outputs**:
- `validation_log`: PipelineValidationLog with:
  - `log_entries`: List of ValidationLogEntry
  - `final_status`: success/partial_success/failure
  - `errors`: List of error messages
  - `warnings`: List of warning messages
  - `summary`: Human-readable summary

**Validation Checks**:
- Email parsing success
- Classification validity
- KG/RAG update status
- Overall pipeline health

---

## 📥 Input Models

### ReceivedEmail
```python
{
  "type": "incoming",  # Literal["incoming"]
  "message_id": "msg-123",
  "thread_id": "thread-456",
  "from_email": "john@example.com",
  "to_emails": ["matt@betafits.com"],
  "cc_emails": [],
  "bcc_emails": [],
  "subject": "Question about your CRM",
  "body": "I'd like to learn more about Betafits...",
  "received_at": "2025-12-24T12:00:00Z",
  "labels": ["INBOX"],
  "is_read": false,
  "attachments": []
}
```

### SentEmail
```python
{
  "type": "outgoing",  # Literal["outgoing"]
  "message_id": "msg-789",
  "thread_id": "thread-456",
  "from_email": "matt@betafits.com",
  "to_emails": ["john@example.com"],
  "subject": "Re: Question about your CRM",
  "body": "Hi John, thanks for your interest...",
  "sent_at": "2025-12-24T12:05:00Z",
  "in_reply_to": "msg-123",
  "sent_status": "sent"
}
```

---

## 📤 Output Model

### PipelineValidationLog
```python
{
  "email_id": "msg-123",
  "pipeline_execution_id": "exec_1703411100000",
  "total_duration_seconds": 3.45,
  "final_status": "success",  # success/partial_success/failure
  "log_entries": [
    {
      "stage": "email_router",
      "status": "success",
      "message": "Email parsed and validated"
    },
    {
      "stage": "email_classifier",
      "status": "success",
      "message": "Classified as 'crm' with 0.95 confidence"
    }
  ],
  "errors": [],
  "warnings": [],
  "summary": "Email processed successfully. Classified as CRM."
}
```

---

## 🚀 Usage

### Via FastAPI Server

```bash
# Start server
uv run apps/email_ingestor_server/main.py

# Process email
curl -X POST http://localhost:3030/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "email_data": {
      "type": "incoming",
      "message_id": "msg-123",
      "from_email": "john@example.com",
      "to_emails": ["matt@betafits.com"],
      "subject": "Question",
      "body": "Hello Betafits...",
      "received_at": "2025-12-24T12:00:00Z"
    }
  }'
```

### Programmatically

```python
from workflows.langgraph.email_processing.graph import process_email
from lib.models.database_schemas import ReceivedEmail

email = ReceivedEmail(
    message_id="msg-123",
    from_email="john@example.com",
    to_emails=["matt@betafits.com"],
    subject="Question",
    body="Hello Betafits...",
    received_at=datetime.now(timezone.utc)
)

result = await process_email(request, email)
print(f"Status: {result['final_status']}")
print(f"Classification: {result['classification']}")
```

---

## 🧪 Testing

Run tests for email processing pipeline:

```bash
# All email processing tests
pytest tests/unit/workflows/email_processing/ -v

# Specific node tests
pytest tests/unit/workflows/email_processing/nodes/test_email_classifier.py -v

# Integration tests
pytest tests/integration/workflows/email_processing/ -v
```

---

## ⚙️ Configuration

Key settings in `lib/config/settings.py`:

```python
# Email Classification
EMAIL_CLASSIFICATION_CONFIDENCE_THRESHOLD = 0.7  # Min confidence for auto-classify
EMAIL_CLASSIFICATION_MODEL = "openai/gpt-4o-mini"

# Knowledge Graph
GRAPHITI_MAX_FACTS = 10  # Max facts returned in RAG queries
NEO4J_BATCH_SIZE = 100  # Batch size for KG updates

# Vector Storage
VECTOR_EMBEDDING_DIMENSION = 1536  # For pgvector
VECTOR_SIMILARITY_THRESHOLD = 0.5  # For semantic search
```

---

## 📊 State Model

```python
class PipelineState(TypedDict, total=False):
    # Input
    parsed_email: ReceivedEmail | SentEmail
    opportunity_id: Optional[str]
    
    # Router output
    is_duplicate: bool
    should_skip: bool
    router_error: Optional[str]
    
    # Classifier output
    labeled_email: LabeledEmail
    classification_error: Optional[str]
    
    # KG/RAG Updater output
    kg_nodes_created: int
    kg_edges_created: int
    rag_vectors_upserted: int
    kg_update_error: Optional[str]
    
    # Validator output
    validation_log: PipelineValidationLog
    
    # Metadata
    pipeline_execution_id: str
    pipeline_start_time: datetime
    config_snapshot: dict[str, Any]
```

---

## 🔍 Error Handling

### Non-Blocking Errors
These errors log warnings but allow pipeline to continue:
- KG/RAG update failures
- Vector embedding errors
- Non-critical validation issues

### Blocking Errors
These errors stop the pipeline:
- Email parsing failures
- Invalid email format
- Database connection errors

### Duplicate Detection
- Checks by `message_id` and `thread_id`
- Prevents duplicate processing
- Logs and skips if found

---

## 📝 Logging

All nodes log with the `[Email Processing Pipeline]` prefix:

```
[Email Processing Pipeline] email_router_node starting for msg-123
[Email Processing Pipeline] Email parsed and validated
[Email Processing Pipeline] email_classifier_node starting
[Email Processing Pipeline] Email classified as 'crm' with 0.95 confidence
[Email Processing Pipeline] kg_rag_updater_node starting
[Email Processing Pipeline] KG updated with 5 nodes, 3 edges
[Email Processing Pipeline] workflow_validator_node complete. Status: success
```

---

## 🛠️ Extending the Pipeline

### Adding a New Node

1. Create node file: `nodes/my_node.py`
2. Implement: `async def my_node(state: PipelineState) -> PipelineState`
3. Add to graph in `graph.py`:
   ```python
   workflow.add_node("my_node", my_node)
   workflow.add_edge("previous_node", "my_node")
   ```
4. Update state model with new fields
5. Add tests in `tests/unit/workflows/email_processing/nodes/`

### Modifying Classification Categories

Edit `lib/models/io_formats.py`:
```python
class EmailCategory(str, Enum):
    CRM = "crm"
    CUSTOMER_SUCCESS = "customer_success"
    SPAM = "spam"
    # Add new category here
    BILLING = "billing"
```

Update classifier prompt in `lib/prompts/email_chains.py`.

---

## 📚 Related Documentation

- [Main README](../../README.md) - Overall pipeline documentation
- [Architecture](../../docs/ARCHITECTURE.md) - System design
- [Database Schema](../../docs/DATABASE_MIGRATION.md) - Email tables
- [Email Drafting Pipeline](../email_drafting/README.md) - Drafting workflow

---

## 💡 Best Practices

1. **Always set `received_at` or `sent_at`** - Required for timestamp tracking
2. **Include thread IDs** - Essential for conversation grouping
3. **Set correct email `type`** - Affects downstream processing
4. **Monitor `classification_confidence`** - Lower scores may need review
5. **Check validation logs** - Always inspect `final_status`

---

## 🚨 Troubleshooting

**Issue**: `is_duplicate` always true
- **Solution**: Check for existing emails in database with same `message_id`

**Issue**: Classification low confidence
- **Solution**: Ensure email body is substantive (>50 chars)
- Check LLM rate limits if failing silently

**Issue**: KG updates failing
- **Solution**: Verify Graphiti/Neo4j connection
- Check `GRAPHITI_API_KEY` in environment

**Issue**: Pipeline timeout
- **Solution**: Reduce `GRAPHITI_MAX_FACTS` if RAG query is slow
- Check OpenRouter API latency

---

## 📞 Support

For issues or questions:
- Check [Troubleshooting](../../docs/TROUBLESHOOTING.md)
- Review test cases in `tests/`
- Open GitHub issue with pipeline execution ID from logs
