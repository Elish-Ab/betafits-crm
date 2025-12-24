"""Configuration and environment management for Betafits email ingestor pipeline.

This module provides centralized configuration management using pydantic BaseSettings.
It supports environment-based configurations (dev/staging/prod) and loads all settings
from environment variables or .env file.

Environment-based configuration:
- ENVIRONMENT=dev: Development settings (local testing, debug logging)
- ENVIRONMENT=staging: Staging settings (realistic data, info logging)
- ENVIRONMENT=prod: Production settings (strict validation, error logging only)

All API keys and sensitive data must be loaded from environment variables.
Never commit secrets to version control.

Usage:
    from lib.config import Settings, get_settings

    settings = get_settings()
    supabase_url = settings.supabase_url
    timeout = settings.request_timeout
"""

import logging
from typing import Literal, Optional, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Centralized settings for the email ingestor pipeline.

    All environment variables are optional at the field level, but some are required
    for actual runtime (enforced via validators). This allows flexibility for testing
    and development environments.

    Attributes:
        environment: Runtime environment (dev/staging/prod). Affects logging/validation.
        debug: Enable debug mode (verbose logging, stack traces).

        # Supabase Configuration
        supabase_url: Supabase project URL (https://xxx.supabase.co).
        supabase_service_key: Service account key (with RLS bypass).
        supabase_anon_key: Public/anon key (optional, for client-side).

        # Graphiti/Neo4j Configuration
        graphiti_api_key: Graphiti API key for knowledge graph operations.
        neo4j_uri: Neo4j connection URI (bolt://localhost:7687).
        neo4j_user: Neo4j username (usually 'neo4j').
        neo4j_password: Neo4j password.

        # OpenRouter LLM Configuration
        openrouter_api_key: OpenRouter API key for LLM access.
        openrouter_base_url: OpenRouter base URL (default: https://openrouter.ai/api/v1).
        openrouter_default_model: Default LLM model (gpt-4o-mini or gpt-5-turbo).

        # Gmail Configuration
        gmail_credentials_path: Path to Gmail service account JSON keyfile.
        gmail_user_email: Email address to send emails from.

        # Request Configuration
        request_timeout: Default timeout for all HTTP requests (seconds).
        request_connect_timeout: Connection timeout (seconds).
        request_read_timeout: Read timeout (seconds).
        request_pool_connections: HTTP connection pool size.
        request_pool_maxsize: Max connections per host.

        # Retry Configuration
        retry_max_attempts: Max retries for transient failures.
        retry_backoff_factor: Exponential backoff multiplier.
        retry_backoff_max: Maximum backoff delay (seconds).
        retry_status_codes: HTTP status codes to retry on.

        # Logging Configuration
        log_level: Logging level (DEBUG/INFO/WARNING/ERROR).
        log_format: Log message format.

        # Feature Flags
        auto_send_emails: Auto-send generated emails or queue for approval.
        enable_kg_updates: Enable knowledge graph updates.
        enable_rag_updates: Enable RAG vector DB updates.
        hipaa_strict_mode: Strict HIPAA filtering (no medical entities extracted).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ========================================================================
    # Environment Configuration
    # ========================================================================

    environment: Literal["dev", "staging", "prod"] = Field(
        default="dev",
        description="Runtime environment (dev/staging/prod)",
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode (verbose logging, stack traces)",
    )

    # ========================================================================
    # Supabase Configuration
    # ========================================================================

    supabase_url: Optional[str] = Field(
        default=None,
        description="Supabase project URL (https://xxx.supabase.co)",
    )
    supabase_service_key: Optional[str] = Field(
        default=None,
        description="Supabase service account key (with RLS bypass)",
    )
    supabase_anon_key: Optional[str] = Field(
        default=None,
        description="Supabase anon/public key (optional, for client-side)",
    )
    supabase_uri: Optional[str] = Field(
        default=None,
        description="Supabase project URi (postgresql://user:pass@host:port/dbname)",
    )
    supabase_schema: Optional[str] = Field(
        default=None,
        description="Supabase custom schema name (e.g., 'crm')",
    )
    vector_table_name: str = Field(
        default="embeddings",
        description="Name of the pgvector table in Supabase for RAG embeddings",
    )

    # ========================================================================
    # Graphiti/Neo4j Configuration
    # ========================================================================

    graphiti_url: Optional[str] = Field(
        default=None,
        description="Graphiti API base URL for knowledge graph operations",
    )
    graphiti_api_key: Optional[str] = Field(
        default=None,
        description="Graphiti API key for knowledge graph operations",
    )
    neo4j_uri: Optional[str] = Field(
        default="bolt://localhost:7687",
        description="Neo4j connection URI",
    )
    neo4j_user: Optional[str] = Field(
        default="neo4j",
        description="Neo4j username",
    )
    neo4j_password: Optional[str] = Field(
        default=None,
        description="Neo4j password",
    )

    # ========================================================================
    # OpenRouter LLM Configuration
    # ========================================================================

    openrouter_api_key: Optional[str] = Field(
        default=None,
        description="OpenRouter API key for LLM access",
    )
    openrouter_base_url: str = Field(
        default="https://openrouter.ai/api/v1",
        description="OpenRouter base URL",
    )
    openrouter_default_model: str = Field(
        default="openai/gpt-4o-mini",
        description="Default LLM model (gpt-4o-mini or gpt-5-turbo)",
    )
    openrouter_embedding_model: str = Field(
        default="thenlper/gte-base",
        description="Embedding model",
    )
    openrouter_embedding_model_786: str = Field(
        default="sentence-transformers/all-mpnet-base-v2",
        description="Embedding model with 786 dimensions",
    )
    openrouter_small_model: str = Field(
        default="mistralai/mistral-small-3.1-24b-instruct:free",
        description="Small model",
    )

    # ========================================================================
    # Gmail Configuration
    # ========================================================================

    gmail_credentials_path: Optional[str] = Field(
        default=None,
        description="Path to Gmail service account JSON keyfile",
    )
    gmail_user_email: Optional[str] = Field(
        default=None,
        description="Email address to send emails from",
    )

    # ========================================================================
    # Request Configuration
    # ========================================================================

    request_timeout: float = Field(
        default=10.0,
        ge=0.1,
        le=300.0,
        description="Default timeout for all HTTP requests (seconds)",
    )
    request_connect_timeout: float = Field(
        default=5.0,
        ge=0.1,
        le=60.0,
        description="Connection timeout (seconds)",
    )
    request_read_timeout: float = Field(
        default=10.0,
        ge=0.1,
        le=300.0,
        description="Read timeout (seconds)",
    )
    request_pool_connections: int = Field(
        default=10,
        ge=1,
        le=100,
        description="HTTP connection pool size",
    )
    request_pool_maxsize: int = Field(
        default=20,
        ge=1,
        le=200,
        description="Max connections per host",
    )

    # ========================================================================
    # Retry Configuration
    # ========================================================================

    retry_max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Max retries for transient failures",
    )
    retry_backoff_factor: float = Field(
        default=2.0,
        ge=1.0,
        le=10.0,
        description="Exponential backoff multiplier",
    )
    retry_backoff_max: float = Field(
        default=60.0,
        ge=1.0,
        le=600.0,
        description="Maximum backoff delay (seconds)",
    )
    retry_status_codes: Union[list[int], str] = Field(
        default=[408, 429, 500, 502, 503, 504],
        description="HTTP status codes to retry on",
    )

    @field_validator("retry_status_codes", mode="before")
    @classmethod
    def parse_retry_status_codes(cls, v: Union[str, list[int]]) -> list[int]:
        """Parse comma-separated string into list of integers."""
        if isinstance(v, str):
            return [int(code.strip()) for code in v.split(",") if code.strip()]
        return v

    # ========================================================================
    # Logging Configuration
    # ========================================================================

    log_level: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)",
    )
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log message format",
    )

    # ========================================================================
    # Feature Flags
    # ========================================================================

    auto_send_emails: bool = Field(
        default=False,
        description="Auto-send generated emails or queue for approval",
    )
    enable_kg_updates: bool = Field(
        default=True,
        description="Enable knowledge graph updates",
    )
    enable_rag_updates: bool = Field(
        default=True,
        description="Enable RAG vector DB updates",
    )
    hipaa_strict_mode: bool = Field(
        default=True,
        description="Strict HIPAA filtering (no medical entities extracted)",
    )

    # ========================================================================
    # Validators
    # ========================================================================

    @field_validator("environment", mode="before")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Normalize and validate environment value."""
        if isinstance(v, str):
            v = v.lower().strip()
        if v not in ("dev", "staging", "prod"):
            raise ValueError(f"environment must be dev/staging/prod, got {v}")
        return v

    @field_validator("supabase_url", mode="after")
    @classmethod
    def validate_supabase_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate Supabase URL format if provided."""
        if v is not None and not v.startswith("https://"):
            raise ValueError(f"Supabase URL must start with https://, got {v}")
        return v

    # @field_validator("openrouter_default_model", mode="before")
    # @classmethod
    # def validate_openrouter_model(cls, v: str) -> str:
    #     """Validate OpenRouter model name."""
    #     valid_models = [
    #         "openai/gpt-oss-20b:free"
    #     ]
    #     if v not in valid_models:
    #         logger.warning(
    #             f"OpenRouter model '{v}' not in known list. "
    #             f"Valid models: {valid_models}"
    #         )
    #     return v

    # ========================================================================
    # Helper Methods
    # ========================================================================

    def is_dev(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "dev"

    def is_staging(self) -> bool:
        """Check if running in staging environment."""
        return self.environment == "staging"

    def is_prod(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "prod"

    def log_config(self) -> None:
        """Log non-sensitive configuration for debugging.

        This logs all settings except API keys and passwords.
        """
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Debug: {self.debug}")
        logger.info(f"Log Level: {self.log_level}")
        logger.info(f"Request Timeout: {self.request_timeout}s")
        logger.info(f"Retry Max Attempts: {self.retry_max_attempts}")
        logger.info(f"Auto Send Emails: {self.auto_send_emails}")
        logger.info(f"HIPAA Strict Mode: {self.hipaa_strict_mode}")


# Global settings instance (singleton pattern)
_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create the global Settings instance.

    This function implements the singleton pattern to ensure only one Settings
    instance is created throughout the application lifecycle.

    Returns:
        Settings: The global settings instance.

    Example:
        >>> settings = get_settings()
        >>> url = settings.supabase_url
        >>> if settings.is_prod():
        ...     print("Running in production")
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
        _settings_instance.log_config()
    return _settings_instance


def reset_settings() -> None:
    """Reset the global settings instance (for testing).

    Warning: This should only be used in tests. Do not call in production.
    """
    global _settings_instance
    _settings_instance = None


# ============================================================================
# Environment-Specific Convenience Functions
# ============================================================================


def get_dev_settings() -> Settings:
    """Get settings configured for development environment.

    Returns:
        Settings: Settings instance with development defaults.
    """
    settings = get_settings()
    if not settings.is_dev():
        raise RuntimeError(
            "Expected development environment but got: " + settings.environment
        )
    return settings


def get_staging_settings() -> Settings:
    """Get settings configured for staging environment.

    Returns:
        Settings: Settings instance with staging defaults.
    """
    settings = get_settings()
    if not settings.is_staging():
        raise RuntimeError(
            "Expected staging environment but got: " + settings.environment
        )
    return settings


def get_prod_settings() -> Settings:
    """Get settings configured for production environment.

    Returns:
        Settings: Settings instance with production defaults.
    """
    settings = get_settings()
    if not settings.is_prod():
        raise RuntimeError(
            "Expected production environment but got: " + settings.environment
        )
    return settings
