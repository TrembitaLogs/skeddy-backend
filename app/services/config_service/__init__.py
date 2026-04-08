"""App configuration service — split into domain-specific modules.

All public symbols are re-exported here so that existing imports
(``from app.services.config_service import ...``) continue to work.
"""

from app.services.config_service.billing import (
    DEFAULT_CREDIT_PRODUCTS,
    DEFAULT_REGISTRATION_BONUS_CREDITS,
    DEFAULT_RIDE_CREDIT_TIERS,
    get_credit_products,
    get_registration_bonus_credits,
    get_ride_credit_tiers,
)
from app.services.config_service.cache import (
    _DB_KEY_TO_CACHE_KEYS,
    CACHE_KEY,
    CACHE_KEY_CLUSTERING_ENABLED,
    CACHE_KEY_CLUSTERING_PENALTY,
    CACHE_KEY_CLUSTERING_REBUILD_INTERVAL,
    CACHE_KEY_CLUSTERING_THRESHOLD,
    CACHE_KEY_CREDIT_PRODUCTS,
    CACHE_KEY_EMAIL_TEMPLATES,
    CACHE_KEY_INTERVAL,
    CACHE_KEY_PUSH_TEMPLATES,
    CACHE_KEY_REGISTRATION_BONUS,
    CACHE_KEY_RIDE_CREDIT_TIERS,
    CACHE_KEY_VERIFICATION_CHECK_INTERVAL,
    CACHE_KEY_VERIFICATION_DEADLINE,
    CACHE_TTL,
    IN_MEMORY_TTL,
    _memory_cache,
    invalidate_config,
)
from app.services.config_service.clustering import (
    DEFAULT_CLUSTERING_ENABLED,
    DEFAULT_CLUSTERING_PENALTY_MINUTES,
    DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES,
    DEFAULT_CLUSTERING_THRESHOLD_MILES,
    get_clustering_enabled,
    get_clustering_penalty_minutes,
    get_clustering_rebuild_interval_minutes,
    get_clustering_threshold_miles,
)
from app.services.config_service.ping import (
    PingConfigs,
    batch_get_ping_configs,
)
from app.services.config_service.search import (
    get_min_search_version,
    get_search_interval_config,
    set_min_search_version,
)
from app.services.config_service.templates import (
    DEFAULT_PUSH_TEMPLATES,
    get_email_templates,
    get_push_templates,
    invalidate_email_templates,
    invalidate_push_templates,
)
from app.services.config_service.verification import (
    DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES,
    DEFAULT_VERIFICATION_DEADLINE_MINUTES,
    get_verification_check_interval_minutes,
    get_verification_deadline_minutes,
)

__all__ = [
    "CACHE_KEY",
    "CACHE_KEY_CLUSTERING_ENABLED",
    "CACHE_KEY_CLUSTERING_PENALTY",
    "CACHE_KEY_CLUSTERING_REBUILD_INTERVAL",
    "CACHE_KEY_CLUSTERING_THRESHOLD",
    "CACHE_KEY_CREDIT_PRODUCTS",
    "CACHE_KEY_EMAIL_TEMPLATES",
    "CACHE_KEY_INTERVAL",
    "CACHE_KEY_PUSH_TEMPLATES",
    "CACHE_KEY_REGISTRATION_BONUS",
    "CACHE_KEY_RIDE_CREDIT_TIERS",
    "CACHE_KEY_VERIFICATION_CHECK_INTERVAL",
    "CACHE_KEY_VERIFICATION_DEADLINE",
    "CACHE_TTL",
    "DEFAULT_CLUSTERING_ENABLED",
    "DEFAULT_CLUSTERING_PENALTY_MINUTES",
    "DEFAULT_CLUSTERING_REBUILD_INTERVAL_MINUTES",
    "DEFAULT_CLUSTERING_THRESHOLD_MILES",
    "DEFAULT_CREDIT_PRODUCTS",
    "DEFAULT_PUSH_TEMPLATES",
    "DEFAULT_REGISTRATION_BONUS_CREDITS",
    "DEFAULT_RIDE_CREDIT_TIERS",
    "DEFAULT_VERIFICATION_CHECK_INTERVAL_MINUTES",
    "DEFAULT_VERIFICATION_DEADLINE_MINUTES",
    "IN_MEMORY_TTL",
    "_DB_KEY_TO_CACHE_KEYS",
    "PingConfigs",
    "_memory_cache",
    "batch_get_ping_configs",
    "get_clustering_enabled",
    "get_clustering_penalty_minutes",
    "get_clustering_rebuild_interval_minutes",
    "get_clustering_threshold_miles",
    "get_credit_products",
    "get_email_templates",
    "get_min_search_version",
    "get_push_templates",
    "get_registration_bonus_credits",
    "get_ride_credit_tiers",
    "get_search_interval_config",
    "get_verification_check_interval_minutes",
    "get_verification_deadline_minutes",
    "invalidate_config",
    "invalidate_email_templates",
    "invalidate_push_templates",
    "set_min_search_version",
]
