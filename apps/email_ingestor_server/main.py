"""FastAPI server for Betafits Email Ingestor Pipeline.

Provides HTTP API endpoints for email ingestion and processing through
the LangGraph pipeline with entity extraction, knowledge graph enrichment,
and AI-powered response generation.

Usage:
    # Run with uvicorn (development)
    uvicorn apps.email_ingestor_server.main:app --host 0.0.0.0 --port 3030 --reload

    # Run with uvicorn (production)
    uvicorn apps.email_ingestor_server.main:app --host 0.0.0.0 --port 3030 --workers 4

    # Run from command line
    python -m apps.email_ingestor_server.main

Environment Variables:
    All configuration loaded from lib.config.settings (reads from .env)
"""

import logging
import signal
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, Union
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from lib.integrations.graphiti_client import get_graphiti_client
from lib.integrations.openrouter_client import get_openrouter_client
from lib.integrations.supabase.supabase_client import get_supabase_client
from lib.models.database_schemas import ReceivedEmail, SentEmail
from lib.models.io_formats import EmailDraftingScenario
from workflows.langgraph.email_drafting.graph import build_email_drafting_graph, draft_email
from workflows.langgraph.email_processing.graph import (
    build_email_processing_graph,
    process_email,
)
from graphiti_core.utils.maintenance.graph_data_operations import clear_data
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================================
# Response Models
# ============================================================================


class EmailProcessingRequest(BaseModel):
    email_data: Union[ReceivedEmail, SentEmail] = Field(
        ..., description="Email data to process", discriminator="type"
    )
    opportunity_id: Optional[str] = None


class IngestResponse(BaseModel):
    """Response model for email ingestion."""

    success: bool = Field(..., description="Whether ingestion was successful")
    email_id: Optional[str] = Field(None, description="Processed email ID")
    sent_status: Optional[str] = Field(
        None, description="Email send status: sent, queued, skipped"
    )
    message_id: Optional[str] = Field(None, description="Gmail message ID")
    duration_seconds: float = Field(..., description="Processing duration in seconds")
    timestamp: str = Field(..., description="Processing timestamp (ISO format)")
    error: Optional[str] = Field(None, description="Error message if failed")


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str = Field(..., description="Service status: healthy, unhealthy")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="API version")
    timestamp: str = Field(..., description="Current timestamp (ISO format)")
    graph_compiled: bool = Field(..., description="Whether LangGraph is compiled")


class ErrorResponse(BaseModel):
    """Response model for errors."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    timestamp: str = Field(..., description="Error timestamp (ISO format)")


# ============================================================================
# Lifespan Management
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan (startup/shutdown)."""

    logger.info("Starting Email Ingestor API Server")

    db_path = "email_processing_checkpoints.db"

    try:
        logger.info(f"[Graph] Connecting to SQLite: {db_path}")
        async with AsyncSqliteSaver.from_conn_string(db_path) as saver:
            app.state.checkpoint_saver = saver
            logger.info("✓ Connected to SQLite checkpoint database")

            email_processing_graph = build_email_processing_graph(saver)
            app.state.email_processing_graph = email_processing_graph
            logger.info("✓ Email processing LangGraph compiled")

            email_drafting_graph = build_email_drafting_graph(saver)
            app.state.email_drafting_graph = email_drafting_graph
            logger.info("✓ Email drafting LangGraph compiled")

            # B. Initialize Graphiti
            try:
                graphiti_client = get_graphiti_client()
                await graphiti_client.build_indices()
                app.state.graphiti_client = graphiti_client
                logger.info("✓ Graphiti client initialized")
            except Exception as e:
                logger.error(f"✗ Failed to initialize Graphiti: {e}")
                # Optional: Decide if Graphiti failure should stop the server
                raise RuntimeError(f"Graphiti initialization failed: {e}") from e

            logger.info("✓ Email Ingestor API Server ready")

            # -----------------------------------------------------------------
            # 3. YIELD (Application Runs Here)
            # -----------------------------------------------------------------
            # The code pauses here. The DB connection remains OPEN.
            yield

            # -----------------------------------------------------------------
            # 4. SHUTDOWN
            # -----------------------------------------------------------------
            logger.info("Shutting down Email Ingestor API Server")

            # A. Close Graphiti
            try:
                if hasattr(app.state, "graphiti_client") and app.state.graphiti_client:
                    logger.info("Closing Graphiti client connection...")
                    await app.state.graphiti_client.close()
                    logger.info("✓ Graphiti client closed")
            except Exception as e:
                logger.error(f"✗ Failed to close Graphiti client: {e}")

            # B. Close Supabase
            try:
                # Assuming get_supabase_client() returns a singleton or active client
                supabase_client = await get_supabase_client()
                if hasattr(supabase_client, "_client") and supabase_client._client:
                    logger.info("Closing Supabase async client...")
                    await supabase_client._client.auth.close()
                    logger.info("✓ Supabase client closed")
            except Exception as e:
                logger.error(f"✗ Failed to close Supabase client: {e}")

            # C. Close OpenRouter
            try:
                openrouter_client = get_openrouter_client()
                # Check for _http or client session attribute depending on your implementation
                if hasattr(openrouter_client, "_http") and openrouter_client._http:
                    logger.info("Closing OpenRouter HTTP connection pool...")
                    openrouter_client._http.clear()  # Often .aclose() for httpx/aiohttp
                    logger.info("✓ OpenRouter connection pool closed")
            except Exception as e:
                logger.error(f"✗ Failed to close OpenRouter client: {e}")

            # D. Close Database (Happens automatically!)
            logger.info("✓ AsyncSQLiteSaver closing automatically via context manager")

    except Exception as e:
        # Catch-all for critical errors during the DB context setup
        logger.error(f"Critical startup failure: {e}")
        raise

    logger.info("✓ All connections closed - shutdown complete")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Betafits Email Ingestor API",
    description="Email processing pipeline with entity extraction, knowledge graph enrichment, and AI-powered responses",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Exception Handlers
# ============================================================================


@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError):
    """Handle Pydantic validation errors."""
    logger.error(f"Validation error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "validation_error",
            "message": "Invalid request data",
            "details": exc.errors(),
            "timestamp": datetime.utcnow().isoformat(),
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "internal_error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat(),
        },
    )


# ============================================================================
# API Routes
# ============================================================================


@app.get("/", response_model=dict[str, str])
async def root():
    """Root endpoint - API information."""
    return {
        "service": "Betafits Email Ingestor API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health",
    }


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint.

    Returns service status and validates that LangGraph is compiled.

    Returns:
        HealthResponse: Service health status
    """

    return HealthResponse(
        status="healthy",
        service="email-ingestor-pipeline",
        version="1.0.0",
        timestamp=datetime.utcnow().isoformat(),
        graph_compiled=True,
    )


@app.get("/api/v1/clear_graph_db", status_code=status.HTTP_200_OK)
async def clear_graph_db():
    """Clear all data from the Graphiti knowledge graph database.

    Returns:
        dict: Result message
    """
    try:
        logger.info("Clearing Graphiti knowledge graph database")
        graphiti_client = get_graphiti_client()
        await clear_data(graphiti_client._graphiti.driver)
        logger.info("✓ Graphiti knowledge graph database cleared successfully")
        return {"message": "Graphiti knowledge graph database cleared successfully"}
    except Exception as e:
        logger.error(f"✗ Failed to clear Graphiti database: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear database: {e}",
        ) from e


@app.get("/api/v1/build_communities", status_code=status.HTTP_200_OK)
async def build_communities():
    """Trigger building communities in Graphiti KG.

    Returns:
        dict: Result message
    """
    try:
        logger.info("Building Graphiti communities")
        graphiti_client = get_graphiti_client()
        await graphiti_client.build_communities()
        logger.info("✓ Graphiti communities built successfully")
        return {"message": "Graphiti communities built successfully"}
    except Exception as e:
        logger.error(f"✗ Failed to build Graphiti communities: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to build communities: {e}",
        ) from e


@app.post("/api/v1/ingest", status_code=status.HTTP_200_OK)
async def ingest_email(request: Request, data: EmailProcessingRequest):
    """Ingest and process an email through the pipeline.

    Accepts raw email data in EmailRaw format and processes it through the
    8-node LangGraph pipeline:
    1. Email Router (parse & dedupe)
    2. Classifier (CRM/Customer Success/Spam)
    3. KG+RAG Update (Graphiti handles entity/relation extraction)
    4. Validator (audit trail)

    Args:
        request (Request): FastAPI request object
        data (EmailProcessingRequest): Email data and processing parameters

    Returns:
        IngestResponse: Processing result with status and metadata

    Raises:
        HTTPException: 400 for validation errors, 500 for processing errors
    """
    start_time = time.time()

    try:
        result = await process_email(request, data.email_data, data.opportunity_id)

        return result

        # # Calculate duration
        # duration = time.time() - start_time

        # # Build response
        # response = IngestResponse(
        #     success=result.get("success", True),
        #     email_id=result.get("email_id"),
        #     sent_status=result.get("sent_status"),
        #     message_id=result.get("message_id"),
        #     duration_seconds=round(duration, 2),
        #     timestamp=datetime.utcnow().isoformat(),
        #     error=result.get("error", None),
        # )

        # logger.info(
        #     f"✓ Email {email_raw.message_id} processed successfully "
        #     f"in {duration:.2f}s (status: {response.sent_status})"
        # )

        # return response

    except ValidationError as ve:
        logger.error(f"Email validation failed: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "validation_error",
                "message": "Invalid email data",
                "details": ve.errors(),
            },
        )
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"✗ Failed to process email {data.email_data.message_id}: {e}",
            exc_info=True,
        )

        # Return error response
        return IngestResponse(
            success=False,
            email_id=None,
            sent_status=None,
            message_id=data.email_data.message_id,
            duration_seconds=round(duration, 2),
            timestamp=datetime.now().isoformat(),
            error=str(e),
        )


@app.post("/api/v1/draft", status_code=status.HTTP_200_OK)
async def draft_email_request(request: Request, data: EmailDraftingScenario):
    start_time = time.time()

    try:
        result = await draft_email(request, data)

        return result

        # # Calculate duration
        # duration = time.time() - start_time

        # # Build response
        # response = IngestResponse(
        #     success=result.get("success", True),
        #     email_id=result.get("email_id"),
        #     sent_status=result.get("sent_status"),
        #     message_id=result.get("message_id"),
        #     duration_seconds=round(duration, 2),
        #     timestamp=datetime.utcnow().isoformat(),
        #     error=result.get("error", None),
        # )

        # logger.info(
        #     f"✓ Email {email_raw.message_id} processed successfully "
        #     f"in {duration:.2f}s (status: {response.sent_status})"
        # )

        # return response

    except ValidationError as ve:
        logger.error(f"Email validation failed: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "validation_error",
                "message": "Invalid email data",
                "details": ve.errors(),
            },
        )
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"✗ Failed to draft email for opportunity {data.opportunity_id}: {e}",
            exc_info=True,
        )


# ============================================================================
# CLI Entry Point
# ============================================================================


def main():
    """Run the FastAPI server with uvicorn.

    Default configuration:
    - Host: 0.0.0.0 (all interfaces)
    - Port: 3030
    - Reload: False (use --reload flag for development)
    """

    logger.info("Starting Email Ingestor API Server via CLI")

    # Install signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=3030,
        reload=False,  # Set to True for development if needed
        log_level="info",
        loop="asyncio",
    )


if __name__ == "__main__":
    main()
