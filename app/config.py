import logging
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _get_app_version() -> str:
    """Get app version from APP_VERSION env var or pyproject.toml."""
    import os

    env_version = os.getenv("APP_VERSION", "")
    if env_version:
        return env_version
    try:
        import tomllib

        pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
        with open(pyproject, "rb") as f:
            data = tomllib.load(f)
        return str(data["project"]["version"])
    except (FileNotFoundError, KeyError, OSError, ValueError):
        return "dev"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    DATABASE_URL: str
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 10

    # Redis
    REDIS_URL: str

    # JWT
    JWT_SECRET: str
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_HOURS: int = 24
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 30

    # Email
    EMAIL_HOST: str = ""
    EMAIL_PORT: int = 587
    EMAIL_USER: str = ""
    EMAIL_PASSWORD: str = ""
    EMAIL_FROM: str = ""

    # Firebase
    FIREBASE_CREDENTIALS_PATH: str = ""
    FIREBASE_CREDENTIALS_JSON: str = ""

    # Google Play Billing
    GOOGLE_PLAY_CREDENTIALS_JSON: str = ""
    GOOGLE_PLAY_CREDENTIALS_PATH: str = ""
    GOOGLE_PLAY_PACKAGE_NAME: str = ""

    # Sentry
    SENTRY_DSN: str = ""

    # App settings
    DEFAULT_SEARCH_INTERVAL_SECONDS: int = 30
    MIN_SEARCH_APP_VERSION: str = "1.0.0"
    SEARCH_APP_UPDATE_URL: str = "https://skeddy-search-releases.sfo3.cdn.digitaloceanspaces.com/search/skeddy-search-latest.apk"

    # Health Monitor
    HEALTH_CHECK_INTERVAL_MINUTES: int = 5
    OFFLINE_NOTIFICATION_THRESHOLD_MINUTES: int = 30

    # CORS
    CORS_ORIGINS: str = ""  # comma-separated allowed origins, e.g. "https://admin.skeddy.app"

    # Admin Panel
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = ""  # bcrypt hash of admin password
    ADMIN_SECRET_KEY: str = ""

    # Backup
    BACKUP_DIR: str = "/backups"

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False

    # Environment identifier (dev, staging, production)
    ENVIRONMENT: str = "dev"

    # App version (set via APP_VERSION env var in production, falls back to pyproject.toml)
    APP_VERSION: str = _get_app_version()

    @model_validator(mode="after")
    def _warn_missing_production_settings(self) -> "Settings":
        """Log warnings for settings that should be set in production."""
        if self.ENVIRONMENT == "dev":
            return self
        _logger = logging.getLogger(__name__)
        if not self.ADMIN_PASSWORD:
            _logger.warning("ADMIN_PASSWORD is not set — admin panel login is disabled")
        if not self.ADMIN_SECRET_KEY:
            _logger.warning("ADMIN_SECRET_KEY is not set — sessions will use an empty key")
        if not self.CORS_ORIGINS:
            raise ValueError("CORS_ORIGINS must be set in production/staging environments")
        if not self.SENTRY_DSN:
            _logger.warning("SENTRY_DSN is not set — error tracking is disabled")
        return self


settings = Settings()
