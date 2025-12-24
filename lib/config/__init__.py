"""Configuration management for Betafits email ingestor pipeline.

This package provides centralized configuration via pydantic BaseSettings.
All settings are loaded from environment variables or .env file.

Usage:
    from lib.config import get_settings

    settings = get_settings()
    supabase_url = settings.supabase_url
    timeout = settings.request_timeout

    # Check environment
    if settings.is_prod():
        print("Running in production")
"""

from .settings import (
    Settings,
    get_dev_settings,
    get_prod_settings,
    get_settings,
    get_staging_settings,
    reset_settings,
)

__all__ = [
    "Settings",
    "get_settings",
    "get_dev_settings",
    "get_staging_settings",
    "get_prod_settings",
    "reset_settings",
]
