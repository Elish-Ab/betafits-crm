# Email Drafting Pipeline

The email drafting pipeline generates personalized, context-aware email drafts using LangChain agents with RAG integration. It creates both response emails and outbound emails based on opportunity context and knowledge base facts.

## 📊 Pipeline Overview

```
Drafting Scenario + Opportunity ID
    ↓
[1] Context Retriever (Fetch opportunity, tone examples, prior emails)
    ↓
[2] Email Drafter (LangChain Agent with Graphiti RAG)
    ↓
[3] Workflow Validator (Audit Trail)
    ↓
DraftedEmail + PipelineValidationLog
```

## 🔄 Nodes

### 1. Context Retriever
**File**: `nodes/context_retriever.py`

**Responsibilities**:
- Fetch opportunity details from Supabase
- Retrieve prior emails (if responding) from database
- Fetch tone guide examples from database
- Query knowledge base for relevant facts
- Build rich ContextBundle

**Inputs**:
- `drafting_scenario`: EmailDraftingScenario
  - `opportunity_id`: UUID of target opportunity
  - `in_reply_to`: Optional message ID of email being responded to
  - `category`: Optional email category (crm, customer_success)

**Outputs**:
- `context_bundle`: ContextBundle with:
  - `opportunity`: Opportunity details
  - `in_reply_to_email`: Optional prior email
  - `response_tone_guide_emails`: List of tone examples
- `context_retrieval_error`: Optional error message

**Error Handling**:
- Non-blocking (pipeline continues with reduced context)
- Gracefully handles missing opportunities or emails
- Provides fallback tone guides if none found

---

### 2. Email Drafter
**File**: `nodes/email_drafter.py`

**Responsibilities**:
- Create LangChain agent with tools
- Query knowledge base using `get_facts_from_memory` tool
- Generate structured email draft using `create_draft_email` tool
- Store draft in database

**LangChain Agent Capabilities**:

#### Tools:

**`get_facts_from_memory(query: str, n_results: int = 10) -> str`**
- Query Graphiti knowledge base for facts
- Returns top N semantically similar facts
- Scoped to opportunity group
- Examples:
  ```
  "Tell me about BlueSky Ventures' portfolio companies"
  "What challenges did we discuss with Lisa?"
  "What are the standard insurance rates for VC-backed startups?"
  ```

**`create_draft_email(...) -> ResponseDraftStructured`**
- Creates final draft with subject, body, recipients, tone
- Called when draft is ready
- Structured output for validation

**LLM Configuration**:
- Model: `openai/gpt-4o-mini` (configurable)
- Temperature: 0.5 (balanced)
- Max tokens: 800

**Drafting Modes**:

#### Response Email (when `context_bundle.in_reply_to_email` exists)
- Subject starts with "Re:"
- Addresses all points from original email
- References prior conversation
- Maintains continuity

#### Outbound Email (when `in_reply_to_email` is None)
- Creates appropriate subject line
- Opens with context from opportunity
- Includes value proposition
- Includes clear call-to-action

**Inputs**:
- `context_bundle`: Retrieved context
- `drafting_scenario`: Drafting instructions and parameters

**Outputs**:
- `response_draft`: DraftedEmail with:
  - `subject`: Email subject
  - `body`: Email body
  - `confidence`: Quality confidence (0.0-1.0)
  - `approval_status`: "pending" (awaiting human approval)
- `draft_error`: Optional error message

---

### 3. Workflow Validator
**File**: `nodes/workflow_validator.py`

**Responsibilities**:
- Validate draft creation success
- Check context retrieval status
- Generate complete audit trail
- Create validation log entry

**Inputs**:
- All state variables from previous nodes

**Outputs**:
- `validation_log`: PipelineValidationLog with:
  - `log_entries`: ValidationLogEntry list
  - `final_status`: success/partial_success/failure
  - `errors`: Error messages
  - `warnings`: Warning messages
  - `summary`: Human-readable summary

**Validation Checks**:
- Draft created successfully
- Confidence score > threshold
- No critical errors occurred

---

## 📥 Input Models

### EmailDraftingScenario
```python
{
  "opportunity_id": "550e8400-e29b-41d4-a716-446655440001",
  "in_reply_to": "msg-123-optional",  # For response emails
  "category": "crm",  # Optional: crm, customer_success
  "from_email": "matt@betafits.com",
  "to_emails": ["john@example.com"],
  "cc_emails": [],
  "bcc_emails": [],
  "drafting_scenario": "Follow up on the CRM demo request. Mention our benchmarking capabilities.",
  "drafting_instructions": "Keep it concise and friendly. Include a call to action for scheduling."
}
```

---

## 📤 Output Models

### DraftedEmail
```python
{
  "subject": "Re: Question about your CRM",
  "body": "Hi John,\n\nThanks for your interest...",
  "from_email": "matt@betafits.com",
  "to_emails": ["john@example.com"],
  "cc_emails": [],
  "bcc_emails": [],
  "model_used": "openai/gpt-4o-mini",
  "confidence": 0.95,
  "approval_status": "pending",
  "metadata": {"tone": "professional"},
  "created_at": "2025-12-24T12:00:00Z"
}
```

### PipelineValidationLog
```python
{
  "email_id": "opp-uuid",
  "pipeline_execution_id": "exec_1703411100000",
  "total_duration_seconds": 5.23,
  "final_status": "success",  # success/partial_success/failure
  "log_entries": [
    {
      "stage": "context_retriever",
      "status": "success",
      "message": "Context retrieved for opportunity opp-uuid"
    },
    {
      "stage": "email_drafter",
      "status": "success",
      "message": "Email draft created successfully with confidence 0.95"
    }
  ],
  "errors": [],
  "warnings": [],
  "summary": "Email draft created successfully..."
}
```

---

## 🚀 Usage

### Via FastAPI Server

```bash
# Start server
uv run apps/email_ingestor_server/main.py

# Draft response email (reply to existing email)
curl -X POST http://localhost:3030/api/v1/draft \
  -H "Content-Type: application/json" \
  -d '{
    "opportunity_id": "550e8400-e29b-41d4-a716-446655440001",
    "in_reply_to": "msg-123",
    "category": "crm",
    "from_email": "matt@betafits.com",
    "to_emails": ["john@example.com"],
    "drafting_scenario": "Follow up on CRM demo request",
    "drafting_instructions": "Be friendly and include scheduling options"
  }'

# Draft outbound email (new email)
curl -X POST http://localhost:3030/api/v1/draft \
  -H "Content-Type: application/json" \
  -d '{
    "opportunity_id": "550e8400-e29b-41d4-a716-446655440001",
    "from_email": "matt@betafits.com",
    "to_emails": ["john@example.com"],
    "drafting_scenario": "Introduction to Betafits for BlueSky Ventures",
    "drafting_instructions": "Highlight VC portfolio benchmarking features"
  }'
```

### Programmatically

```python
from workflows.langgraph.email_drafting.graph import draft_email
from lib.models.io_formats import EmailDraftingScenario

scenario = EmailDraftingScenario(
    opportunity_id="550e8400-e29b-41d4-a716-446655440001",
    in_reply_to="msg-123",  # For response email
    from_email="matt@betafits.com",
    to_emails=["john@example.com"],
    drafting_scenario="Follow up on CRM demo",
    drafting_instructions="Be friendly"
)

result = await draft_email(request, scenario)
print(f"Draft subject: {result['response_draft'].subject}")
print(f"Draft confidence: {result['response_draft'].confidence}")
```

---

## 🧠 Agent Flow Example

```
User Request: Draft response to inquiry about CRM
↓
[Context Retriever]
  - Fetches BlueSky Ventures opportunity details
  - Retrieves original inquiry email from John
  - Gets 3 tone guide examples from similar deals
  - Builds ContextBundle
↓
[Email Drafter Agent Start]
  Agent: "I need to understand BlueSky Ventures better"
  ↓
  Agent calls: get_facts_from_memory("BlueSky Ventures portfolio companies")
  ← Returns: "BlueSky has invested in 15 healthcare startups..."
  ↓
  Agent calls: get_facts_from_memory("John's previous concerns with insurance")
  ← Returns: "John asked about PEO integration..."
  ↓
  Agent now has enough context
  ↓
  Agent calls: create_draft_email(
    subject="Re: CRM Solution for BlueSky Portfolio",
    body="Hi John,\n\nThanks for your interest...",
    to_emails=["john@example.com"],
    tone="professional",
    confidence=0.95
  )
  ← Returns: ResponseDraftStructured
↓
[Workflow Validator]
  - Validates draft created successfully
  - Confidence: 0.95 ✓
  - No errors
  - Creates validation log
↓
DraftedEmail returned to user
Status: pending (awaiting human approval)
```

---

## 🎯 Best Practices

### Drafting Scenario Tips

**Good Examples**:
```python
# Specific and contextual
"Follow up on demo request from last week. 
 Mention the 20% savings case study we discussed."

"Introduction to Betafits as a strategic addition 
 to their portfolio support. Highlight benchmarking."

"Response to concerns about HIPAA compliance. 
 Reference our SOC 2 certification."
```

**Avoid**:
```python
# Too vague
"Send an email about CRM"

# Too prescriptive (let LLM decide)
"Say hello and introduce yourself and mention 
 our features and ask for a call"
```

### Using `get_facts_from_memory`

**Effective Queries**:
- Company/contact specific: `"Who is Lisa Gray and what's her role at BlueSky?"`
- Topic specific: `"What are standard insurance rates for VC-backed startups?"`
- Context specific: `"What challenges did this company face with their last PEO?"`

**Less Effective**:
- Too broad: `"Tell me everything about this company"`
- Too narrow: `"John's email"`

---

## 🧪 Testing

Run tests for email drafting pipeline:

```bash
# All email drafting tests
pytest tests/unit/workflows/email_drafting/ -v

# Specific node tests
pytest tests/unit/workflows/email_drafting/nodes/test_email_drafter.py -v

# Integration tests
pytest tests/integration/workflows/email_drafting/ -v

# With coverage
pytest tests/unit/workflows/email_drafting/ --cov=workflows.langgraph.email_drafting
```

---

## ⚙️ Configuration

Key settings in `lib/config/settings.py`:

```python
# LLM Configuration
OPENROUTER_DEFAULT_MODEL = "openai/gpt-4o-mini"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Agent Configuration
AGENT_TEMPERATURE = 0.5  # Balanced (creative but coherent)
AGENT_MAX_TOKENS = 800  # Draft size limit

# Knowledge Base
GRAPHITI_MAX_FACTS = 10  # Facts returned per query
GRAPHITI_SIMILARITY_THRESHOLD = 0.5  # Relevance cutoff

# Draft Validation
DRAFT_CONFIDENCE_THRESHOLD = 0.7  # Min confidence to auto-approve
```

---

## 📊 State Model

```python
class PipelineState(TypedDict, total=False):
    # Input
    drafting_scenario: EmailDraftingScenario
    
    # Context Retriever output
    context_bundle: ContextBundle
    context_retrieval_error: Optional[str]
    
    # Email Drafter output
    response_draft: DraftedEmail
    draft_error: Optional[str]
    
    # Validator output
    validation_log: PipelineValidationLog
    
    # Metadata
    pipeline_execution_id: str
    pipeline_start_time: datetime
    pipeline_failed: bool
    error_message: Optional[str]
    config_snapshot: dict[str, Any]
    model_responses: dict[str, Any]
```

---

## 🔍 Error Handling

### Non-Blocking Errors
- Missing tone guide examples (provides defaults)
- No prior emails found (creates outbound draft)
- Graphiti facts not found (drafts with available context)

### Blocking Errors
- Opportunity not found (cannot proceed)
- Invalid email addresses (validation fails)
- LLM API failure (cannot generate draft)

### Confidence Scoring
- Ranges from 0.0 (low confidence) to 1.0 (high confidence)
- Based on LLM model output
- Can be used to auto-approve drafts (`confidence > threshold`)

---

## 📝 Logging

All nodes log with the `[Email Drafting Pipeline]` prefix:

```
[Email Drafting Pipeline] context_retriever_node starting for opportunity ID opp-123
[Email Drafting Pipeline] Opportunity fetched: BlueSky Ventures
[Email Drafting Pipeline] Retrieved 3 tone guide emails
[Email Drafting Pipeline] email_drafter_node starting
[Email Drafting Pipeline] Invoking response drafter agent
[Email Drafting Pipeline] Agent queried knowledge base for facts
[Email Drafting Pipeline] Draft generated with confidence 0.95
[Email Drafting Pipeline] Validation complete. Status: success
```

---

## 🛠️ Extending the Pipeline

### Adding Custom Agent Tools

1. Define tool in `email_drafter_node`:
   ```python
   @tool
   async def my_custom_tool(query: str) -> str:
       """Custom tool for drafting."""
       # Implementation
       return result
   ```

2. Add to agent tools list:
   ```python
   response_drafter_agent = create_agent(
       model=llm,
       tools=[create_draft_email, get_facts_from_memory, my_custom_tool],
       system_prompt=EMAIL_DRAFT_SYSTEM_PROMPT,
   )
   ```

3. Update system prompt to encourage usage

### Modifying Tone Guidelines

Edit `lib/prompts/email_chains.py`:
```python
RESPONSE_DRAFT_EXAMPLES = [
    {
        "type": "response",
        "email": "Your example email",
        "response": "{'subject': '...', 'body': '...', 'tone': 'professional'}"
    }
]
```

---

## 📚 Related Documentation

- [Main README](../../README.md) - Overall project documentation
- [Email Processing Pipeline](../email_processing/README.md) - Processing workflow
- [Architecture](../../docs/ARCHITECTURE.md) - System design
- [Deployment](../../docs/DEPLOYMENT.md) - Production guide

---

## 💡 Use Cases

### 1. Auto-Response to Sales Inquiries
```python
scenario = EmailDraftingScenario(
    opportunity_id="opp-uuid",
    in_reply_to="msg-inquiry",
    category="crm",
    from_email="sales@betafits.com",
    to_emails=["prospect@company.com"],
    drafting_scenario="Prospect asked about our CRM",
    drafting_instructions="Schedule a demo"
)
```

### 2. Follow-up on Inactive Opportunities
```python
scenario = EmailDraftingScenario(
    opportunity_id="opp-uuid",
    # No in_reply_to → creates outbound email
    from_email="success@betafits.com",
    to_emails=["contact@company.com"],
    drafting_scenario="Customer Success follow-up after 2 months",
    drafting_instructions="Check in on their usage and ROI"
)
```

### 3. Multi-Recipient Communication
```python
scenario = EmailDraftingScenario(
    opportunity_id="opp-uuid",
    from_email="matt@betafits.com",
    to_emails=["cfo@company.com", "cto@company.com"],
    cc_emails=["success@company.com"],
    drafting_scenario="Quarterly business review for VP audience",
    drafting_instructions="Include metrics and recommendations"
)
```

---

## 🚨 Troubleshooting

**Issue**: Draft takes too long to generate
- **Solution**: Reduce `GRAPHITI_MAX_FACTS` (fewer knowledge base queries)
- Check LangChain agent streaming logs

**Issue**: Low confidence scores
- **Solution**: Provide more detailed `drafting_instructions`
- Ensure context_bundle has tone guide examples
- Check that opportunity has sufficient historical context

**Issue**: Agent not using `get_facts_from_memory`
- **Solution**: System prompt emphasizes tool usage
- Add more context to drafting_scenario
- Check Graphiti connection

**Issue**: Generated draft too long/short
- **Solution**: Adjust `AGENT_MAX_TOKENS` setting
- Provide length guidance in `drafting_instructions`

**Issue**: Email formatting issues
- **Solution**: Verify `ResponseDraftStructured` validation
- Check LLM response format compliance

---

## 📞 Support

For issues or questions:
- Check [Troubleshooting](../../docs/TROUBLESHOOTING.md)
- Review test cases in `tests/unit/workflows/email_drafting/`
- Inspect agent logs with `stream_mode="values"`
- Open GitHub issue with pipeline execution ID from logs
