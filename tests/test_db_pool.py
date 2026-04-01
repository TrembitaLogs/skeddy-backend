"""Verify database engine is configured with connection pool parameters."""

from app.database import engine


def test_pool_size_configured():
    """Engine pool_size matches the configured setting."""
    assert engine.pool.size() == 5  # default DB_POOL_SIZE


def test_max_overflow_configured():
    """Engine max_overflow matches the configured setting."""
    assert engine.pool._max_overflow == 10  # default DB_MAX_OVERFLOW


def test_pool_pre_ping_enabled():
    """pool_pre_ping is enabled to detect stale connections."""
    assert engine.pool._pre_ping is True
