# Betafits CRM

## Overview
Betafits CRM is the main domain repository containing all CRM-related business logic, workflows, and integrations including Form 5500 data processing capabilities.

## Structure

### apps/
Application entrypoints and executables:
- `crm_api/` - Main CRM API server
- `crm_worker/` - Background worker processes
- `email_ingestor_server/` - Email ingestion service
- `form5500_ingestion_cli/` - CLI for Form 5500 data pipelines

### services/
Business logic and integrations:
- `crm_intel/` - CRM intelligence services
- `email_intel/` - Email processing and analysis
- `enrichment/` - Data enrichment modules
- `vc_matching/` - VC portfolio matching
- `form5500_legacy/` - Form 5500 legacy processing scripts

### workflows/
LangGraph orchestration workflows:
- `langgraph/crm_brain/` - Main CRM orchestration
- `langgraph/email_processing/` - Email workflow
- `langgraph/meeting_flow/` - Meeting processing
- `langgraph/enrichment_flow/` - Data enrichment
- `langgraph/form5500_flow/` - Form 5500 ingestion workflow

### lib/
Shared foundational code:
- `config/` - Configuration modules
- `models/` - Pydantic models and state definitions
- `integrations/` - External service adapters
- `utils/` - Utility functions
- `prompts/` - LLM prompts

### frontend/
React-based CRM portal and UI

## Dependency Direction
Following Betafits Engineering Standards:
```
lib → services → workflows → apps
```

## Form 5500 Integration
The Form 5500 processing capabilities have been integrated from CRM_5500s following Betafits Engineering Standards:
- Legacy processing code: `services/form5500_legacy/`
- LangGraph workflow: `workflows/langgraph/form5500_flow/`
- CLI application: `apps/form5500_ingestion_cli/`
- Configuration: `lib/config/form5500_config.py`
- State models: `lib/models/form5500_state.py`

