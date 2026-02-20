from pathlib import Path

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
    except Exception:
        return "dev"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    DATABASE_URL: str

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

    # Sentry
    SENTRY_DSN: str = ""

    # App settings
    DEFAULT_SEARCH_INTERVAL_SECONDS: int = 30
    MIN_SEARCH_APP_VERSION: str = "1.0.0"
    SEARCH_APP_UPDATE_URL: str = "https://skeddy.net/download/search-app.apk"

    # Health Monitor
    HEALTH_CHECK_INTERVAL_MINUTES: int = 5
    OFFLINE_NOTIFICATION_THRESHOLD_MINUTES: int = 30

    # Admin Panel
    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = ""
    ADMIN_SECRET_KEY: str = ""

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False

    # App version (set via APP_VERSION env var in production, falls back to pyproject.toml)
    APP_VERSION: str = _get_app_version()


settings = Settings()
