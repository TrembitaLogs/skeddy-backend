"""Unit tests for AdminAuth authentication backend."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.admin.auth import AdminAuth
from app.config import settings


class TestAdminAuthLogin:
    """Tests for AdminAuth.login() method."""

    @pytest.fixture
    def auth_backend(self):
        """Provide AdminAuth instance for testing."""
        return AdminAuth(secret_key="test-secret-key")

    @pytest.fixture
    def mock_request(self):
        """Provide a mocked Request object."""
        request = MagicMock()
        request.session = {}
        return request

    async def test_login_returns_true_with_correct_credentials(
        self, auth_backend, mock_request, monkeypatch
    ):
        """Test AdminAuth.login() returns True with correct credentials."""
        # Set test credentials via monkeypatch
        test_username = "test_admin"
        test_password = "test_secure_password"
        monkeypatch.setattr(settings, "ADMIN_USERNAME", test_username)
        monkeypatch.setattr(settings, "ADMIN_PASSWORD", test_password)

        # Mock form data with correct credentials
        mock_form_data = {
            "username": test_username,
            "password": test_password,
        }
        mock_request.form = AsyncMock(return_value=mock_form_data)

        result = await auth_backend.login(mock_request)

        assert result is True
        assert mock_request.session.get("admin_authenticated") is True

    async def test_login_returns_false_with_wrong_username(
        self, auth_backend, mock_request, monkeypatch
    ):
        """Test AdminAuth.login() returns False with wrong username."""
        test_username = "test_admin"
        test_password = "test_secure_password"
        monkeypatch.setattr(settings, "ADMIN_USERNAME", test_username)
        monkeypatch.setattr(settings, "ADMIN_PASSWORD", test_password)

        # Mock form data with wrong username
        mock_form_data = {
            "username": "wrong_admin",
            "password": test_password,
        }
        mock_request.form = AsyncMock(return_value=mock_form_data)

        result = await auth_backend.login(mock_request)

        assert result is False
        assert mock_request.session.get("admin_authenticated") is None

    async def test_login_returns_false_with_wrong_password(
        self, auth_backend, mock_request, monkeypatch
    ):
        """Test AdminAuth.login() returns False with wrong password."""
        test_username = "test_admin"
        test_password = "test_secure_password"
        monkeypatch.setattr(settings, "ADMIN_USERNAME", test_username)
        monkeypatch.setattr(settings, "ADMIN_PASSWORD", test_password)

        # Mock form data with wrong password
        mock_form_data = {
            "username": test_username,
            "password": "wrong_password",
        }
        mock_request.form = AsyncMock(return_value=mock_form_data)

        result = await auth_backend.login(mock_request)

        assert result is False
        assert mock_request.session.get("admin_authenticated") is None

    async def test_login_returns_false_with_wrong_both_credentials(
        self, auth_backend, mock_request, monkeypatch
    ):
        """Test AdminAuth.login() returns False with both wrong credentials."""
        test_username = "test_admin"
        test_password = "test_secure_password"
        monkeypatch.setattr(settings, "ADMIN_USERNAME", test_username)
        monkeypatch.setattr(settings, "ADMIN_PASSWORD", test_password)

        # Mock form data with both wrong credentials
        mock_form_data = {
            "username": "wrong_admin",
            "password": "wrong_password",
        }
        mock_request.form = AsyncMock(return_value=mock_form_data)

        result = await auth_backend.login(mock_request)

        assert result is False
        assert mock_request.session.get("admin_authenticated") is None


class TestAdminAuthLogout:
    """Tests for AdminAuth.logout() method."""

    @pytest.fixture
    def auth_backend(self):
        """Provide AdminAuth instance for testing."""
        return AdminAuth(secret_key="test-secret-key")

    @pytest.fixture
    def mock_request(self):
        """Provide a mocked Request object."""
        request = MagicMock()
        request.session = {"admin_authenticated": True, "other_key": "value"}
        return request

    async def test_logout_clears_session(self, auth_backend, mock_request):
        """Test AdminAuth.logout() clears session."""
        # Verify session has data before logout
        assert mock_request.session == {"admin_authenticated": True, "other_key": "value"}

        result = await auth_backend.logout(mock_request)

        assert result is True
        # Verify session is cleared after logout
        assert mock_request.session == {}


class TestAdminAuthAuthenticate:
    """Tests for AdminAuth.authenticate() method."""

    @pytest.fixture
    def auth_backend(self):
        """Provide AdminAuth instance for testing."""
        return AdminAuth(secret_key="test-secret-key")

    @pytest.fixture
    def mock_request(self):
        """Provide a mocked Request object."""
        request = MagicMock()
        return request

    async def test_authenticate_returns_true_when_session_has_admin_authenticated_true(
        self, auth_backend, mock_request
    ):
        """Test AdminAuth.authenticate() returns True when session has admin_authenticated=True."""
        mock_request.session = {"admin_authenticated": True}

        result = await auth_backend.authenticate(mock_request)

        assert result is True

    async def test_authenticate_returns_false_when_session_is_empty(
        self, auth_backend, mock_request
    ):
        """Test AdminAuth.authenticate() returns False when session is empty."""
        mock_request.session = {}

        result = await auth_backend.authenticate(mock_request)

        assert result is False

    async def test_authenticate_returns_false_when_admin_authenticated_is_false(
        self, auth_backend, mock_request
    ):
        """Test AdminAuth.authenticate() returns False when admin_authenticated is False."""
        mock_request.session = {"admin_authenticated": False}

        result = await auth_backend.authenticate(mock_request)

        assert result is False
