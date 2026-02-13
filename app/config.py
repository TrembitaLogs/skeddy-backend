from pydantic_settings import BaseSettings, SettingsConfigDict


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


settings = Settings()
