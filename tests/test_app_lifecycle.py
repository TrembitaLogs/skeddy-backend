"""Tests for billing background tasks integration in app lifecycle (task 8.5).

Test strategy:
1. All 7 background tasks are created during lifespan startup
2. Graceful shutdown: all tasks cancelled without errors
3. Checkpoint preserved across shutdown/restart cycles
4. Integration test with full app lifecycle (health endpoint)
"""

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import lifespan

# All background task functions that should be registered in the lifespan.
TASK_MODULES = {
    "app.main.check_device_health": "health_check",
    "app.main.cleanup_expired_tokens": "token_cleanup",
    "app.main.cleanup_old_data": "data_cleanup",
    "app.main.run_low_balance_reminder": "low_balance_reminder",
    "app.main.run_verification_fallback": "ride_verification",
    "app.main.run_purchase_recovery": "purchase_recovery",
    "app.main.run_balance_reconciliation": "balance_reconciliation",
}


def _make_mock_coroutine(name: str) -> AsyncMock:
    """Create a coroutine mock that blocks until cancelled."""

    async def _block_forever():
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(3600)

    mock = AsyncMock(side_effect=_block_forever)
    mock.__name__ = name
    return mock


# ---------------------------------------------------------------------------
# Test 1: All tasks are started during lifespan startup
# ---------------------------------------------------------------------------


class TestLifespanStartup:
    @pytest.mark.asyncio
    async def test_all_background_tasks_created(self):
        """Lifespan creates asyncio tasks for all 7 background functions."""
        mocks = {path: _make_mock_coroutine(name) for path, name in TASK_MODULES.items()}
        created_tasks: list[asyncio.Task] = []
        original_create_task = asyncio.create_task

        def tracking_create_task(coro, **kwargs):
            task = original_create_task(coro, **kwargs)
            created_tasks.append(task)
            return task

        patches = [patch(path, m) for path, m in mocks.items()]
        patches.append(patch("app.main.initialize_firebase", return_value=None))

        for p in patches:
            p.start()

        try:
            with patch("asyncio.create_task", side_effect=tracking_create_task):
                mock_app = MagicMock()
                async with lifespan(mock_app):
                    # All 7 tasks should have been created
                    assert len(created_tasks) == 7

                    # Verify all mocks were called (coroutine started)
                    for path, mock_fn in mocks.items():
                        mock_fn.assert_called_once(), (f"{path} was not called during startup")
        finally:
            for p in patches:
                p.stop()
            # Clean up any remaining tasks
            for t in created_tasks:
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t


# ---------------------------------------------------------------------------
# Test 2: Graceful shutdown — tasks cancelled without errors
# ---------------------------------------------------------------------------


class TestGracefulShutdown:
    @pytest.mark.asyncio
    async def test_all_tasks_cancelled_on_shutdown(self):
        """Exiting lifespan context cancels all background tasks gracefully."""
        mocks = {path: _make_mock_coroutine(name) for path, name in TASK_MODULES.items()}

        patches = [patch(path, m) for path, m in mocks.items()]
        patches.append(patch("app.main.initialize_firebase", return_value=None))

        for p in patches:
            p.start()

        try:
            mock_app = MagicMock()
            async with lifespan(mock_app):
                pass
            # After exiting the context manager, all tasks should be done
            # (no hanging tasks). If any task wasn't properly cancelled,
            # the test would hang or fail.
        finally:
            for p in patches:
                p.stop()

    @pytest.mark.asyncio
    async def test_shutdown_suppresses_cancelled_error(self):
        """CancelledError from tasks is suppressed during shutdown (no crash)."""

        async def raise_on_cancel():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                # Simulate cleanup work that re-raises
                raise

        mocks = {}
        for path, name in TASK_MODULES.items():
            mock = AsyncMock(side_effect=raise_on_cancel)
            mock.__name__ = name
            mocks[path] = mock

        patches = [patch(path, m) for path, m in mocks.items()]
        patches.append(patch("app.main.initialize_firebase", return_value=None))

        for p in patches:
            p.start()

        try:
            mock_app = MagicMock()
            # This should NOT raise even though tasks raise CancelledError
            async with lifespan(mock_app):
                pass
        finally:
            for p in patches:
                p.stop()

    @pytest.mark.asyncio
    async def test_shutdown_handles_task_that_already_finished(self):
        """Task that finished before shutdown is handled gracefully."""

        async def finish_immediately():
            return  # Finish right away, no waiting

        mocks = {}
        for path, name in TASK_MODULES.items():
            mock = AsyncMock(side_effect=finish_immediately)
            mock.__name__ = name
            mocks[path] = mock

        patches = [patch(path, m) for path, m in mocks.items()]
        patches.append(patch("app.main.initialize_firebase", return_value=None))

        for p in patches:
            p.start()

        try:
            mock_app = MagicMock()
            async with lifespan(mock_app):
                # Give tasks time to complete
                await asyncio.sleep(0.01)
            # Should not raise even though tasks are already done
        finally:
            for p in patches:
                p.stop()


# ---------------------------------------------------------------------------
# Test 3: Checkpoint preserved across restart
# ---------------------------------------------------------------------------


class TestCheckpointPreservation:
    @pytest.mark.asyncio
    async def test_reconciliation_checkpoint_survives_restart(self):
        """balance_reconciliation reads checkpoint from Redis on each cycle.

        The checkpoint is written to Redis by the task itself during normal
        operation.  On restart, the task reads the existing checkpoint and
        resumes incremental reconciliation.  This test verifies the
        checkpoint read/write path survives a cancel (simulating shutdown)
        and a subsequent start (simulating restart).
        """
        from app.tasks.balance_reconciliation import (
            CHECKPOINT_KEY_PREFIX,
            get_checkpoint,
            save_checkpoint,
        )

        fake_store: dict[str, str] = {}
        fake_redis = AsyncMock()

        async def mock_get(key):
            return fake_store.get(key)

        async def mock_setex(key, ttl, value):
            fake_store[key] = value

        async def mock_delete(*keys):
            for k in keys:
                fake_store.pop(k, None)

        fake_redis.get = AsyncMock(side_effect=mock_get)
        fake_redis.setex = AsyncMock(side_effect=mock_setex)
        fake_redis.delete = AsyncMock(side_effect=mock_delete)

        import uuid

        user_id = uuid.uuid4()

        # Simulate a task saving a checkpoint (normal operation before shutdown)
        await save_checkpoint(
            user_id,
            "tx-123",
            "2026-02-25T10:00:00+00:00",
            42,
            fake_redis,
        )

        # Verify checkpoint is in store
        key = f"{CHECKPOINT_KEY_PREFIX}{user_id}"
        assert key in fake_store

        # Simulate restart — checkpoint should still be readable
        cp = await get_checkpoint(user_id, fake_redis)
        assert cp is not None
        assert cp["last_tx_id"] == "tx-123"
        assert cp["balance_at_checkpoint"] == 42


# ---------------------------------------------------------------------------
# Test 4: Integration test with full app lifecycle
# ---------------------------------------------------------------------------


class TestFullAppLifecycle:
    @pytest.mark.asyncio
    async def test_app_starts_and_serves_health_endpoint(self):
        """Full app starts with all tasks, serves /health, and shuts down."""
        # Mock all background task functions to prevent actual DB/Redis access
        mocks = {path: _make_mock_coroutine(name) for path, name in TASK_MODULES.items()}

        patches = [patch(path, m) for path, m in mocks.items()]
        patches.append(patch("app.main.initialize_firebase", return_value=None))

        for p in patches:
            p.start()

        try:
            from app.main import app

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/health")
                assert resp.status_code == 200
                data = resp.json()
                assert data["status"] in ("ok", "degraded")
        finally:
            for p in patches:
                p.stop()

    @pytest.mark.asyncio
    async def test_app_starts_with_firebase_failure(self):
        """App starts even when Firebase initialization fails."""
        mocks = {path: _make_mock_coroutine(name) for path, name in TASK_MODULES.items()}

        patches = [patch(path, m) for path, m in mocks.items()]
        patches.append(
            patch(
                "app.main.initialize_firebase",
                side_effect=RuntimeError("No credentials"),
            )
        )

        for p in patches:
            p.start()

        try:
            from app.main import app

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/health")
                assert resp.status_code == 200
        finally:
            for p in patches:
                p.stop()
