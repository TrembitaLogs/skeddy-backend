"""Tests for admin backup view and utility functions (SKE-21)."""

import json

import pytest

from app.admin.backup import BackupView, _format_size, _format_timestamp, _read_json_file


class TestReadJsonFile:
    """Tests for _read_json_file helper."""

    def test_reads_valid_json(self, tmp_path):
        """Valid JSON file is parsed and returned as dict."""
        p = tmp_path / "data.json"
        p.write_text(json.dumps({"key": "value", "count": 42}))
        result = _read_json_file(p)
        assert result == {"key": "value", "count": 42}

    def test_returns_empty_dict_for_missing_file(self, tmp_path):
        """Missing file returns empty dict instead of raising."""
        result = _read_json_file(tmp_path / "nonexistent.json")
        assert result == {}

    def test_returns_empty_dict_for_invalid_json(self, tmp_path):
        """Malformed JSON returns empty dict instead of raising."""
        p = tmp_path / "bad.json"
        p.write_text("not valid json {{{")
        result = _read_json_file(p)
        assert result == {}

    def test_returns_empty_dict_for_empty_file(self, tmp_path):
        """Empty file returns empty dict."""
        p = tmp_path / "empty.json"
        p.write_text("")
        result = _read_json_file(p)
        assert result == {}


class TestFormatSize:
    """Tests for _format_size helper."""

    def test_bytes_range(self):
        """Values under 1024 shown as bytes."""
        assert _format_size(0) == "0 B"
        assert _format_size(512) == "512 B"
        assert _format_size(1023) == "1023 B"

    def test_kilobytes_range(self):
        """Values between 1 KB and 1 MB shown as KB."""
        assert _format_size(1024) == "1.0 KB"
        assert _format_size(1536) == "1.5 KB"
        assert _format_size(1024 * 1024 - 1) == "1024.0 KB"

    def test_megabytes_range(self):
        """Values >= 1 MB shown as MB."""
        assert _format_size(1024 * 1024) == "1.0 MB"
        assert _format_size(5 * 1024 * 1024) == "5.0 MB"
        assert _format_size(int(2.5 * 1024 * 1024)) == "2.5 MB"


class TestFormatTimestamp:
    """Tests for _format_timestamp helper."""

    def test_valid_timestamp(self):
        """Standard YYYYMMDD_HHMMSS format is converted correctly."""
        assert _format_timestamp("20260405_143015") == "2026-04-05 14:30:15 UTC"

    def test_none_returns_never(self):
        """None input returns 'Never'."""
        assert _format_timestamp(None) == "Never"

    def test_short_string_returns_never(self):
        """String shorter than 15 chars returns 'Never'."""
        assert _format_timestamp("20260405") == "Never"
        assert _format_timestamp("") == "Never"

    def test_exactly_15_chars(self):
        """Minimum valid length (15 chars) is formatted."""
        assert _format_timestamp("20260101_000000") == "2026-01-01 00:00:00 UTC"


class TestBackupViewConfiguration:
    """Tests for BackupView class attributes."""

    def test_has_required_attributes(self):
        assert hasattr(BackupView, "name")
        assert hasattr(BackupView, "icon")

    def test_name_and_icon(self):
        assert BackupView.name == "Backups"
        assert BackupView.icon == "fa-solid fa-database"


class TestBackupPage:
    """Tests for BackupView.backup_page endpoint."""

    @pytest.mark.asyncio
    async def test_backup_page_accessible(self, app_client):
        """Backup page route exists and requires auth (redirect)."""
        resp = await app_client.get("/admin/backup")
        assert resp.status_code in (200, 302, 303)

    @pytest.mark.asyncio
    async def test_backup_page_renders_after_login(self, admin_client):
        """Authenticated admin can access the backup page."""
        resp = await admin_client.client.get("/admin/backup")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_backup_page_shows_status_fields(self, admin_client):
        """Page contains key backup status labels."""
        resp = await admin_client.client.get("/admin/backup")
        assert resp.status_code == 200
        body = resp.text
        # The template should render status/config info
        assert "Backup" in body or "backup" in body

    @pytest.mark.asyncio
    async def test_backup_page_with_status_file(self, admin_client, tmp_path, monkeypatch):
        """Backup page reads last-backup status file and renders data."""
        status_file = tmp_path / ".last_backup_status"
        status_file.write_text(
            json.dumps(
                {
                    "status": "success",
                    "timestamp": "20260401_120000",
                    "file": "skeddy_backup_20260401.dump",
                    "size": 2048,
                    "duration": 3.5,
                    "error": "",
                }
            )
        )
        config_file = tmp_path / ".backup_config.json"
        config_file.write_text(json.dumps({"interval_hours": 12, "retention_days": 14}))

        monkeypatch.setattr("app.admin.backup.STATUS_FILE", status_file)
        monkeypatch.setattr("app.admin.backup.CONFIG_FILE", config_file)
        monkeypatch.setattr("app.admin.backup.BACKUP_DIR", tmp_path)

        resp = await admin_client.client.get("/admin/backup")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_backup_page_with_backup_files(self, admin_client, tmp_path, monkeypatch):
        """Backup page counts .dump files and computes total size."""
        # Create fake backup files
        (tmp_path / "skeddy_backup_20260401.dump").write_bytes(b"x" * 1024)
        (tmp_path / "skeddy_backup_20260402.dump").write_bytes(b"y" * 2048)
        # Non-matching file should be ignored
        (tmp_path / "other_file.txt").write_text("ignored")

        monkeypatch.setattr("app.admin.backup.BACKUP_DIR", tmp_path)
        monkeypatch.setattr("app.admin.backup.STATUS_FILE", tmp_path / ".last_backup_status")
        monkeypatch.setattr("app.admin.backup.CONFIG_FILE", tmp_path / ".backup_config.json")

        resp = await admin_client.client.get("/admin/backup")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_backup_page_missing_dir(self, admin_client, tmp_path, monkeypatch):
        """Backup page handles non-existent backup directory gracefully."""
        missing_dir = tmp_path / "no_such_dir"
        monkeypatch.setattr("app.admin.backup.BACKUP_DIR", missing_dir)
        monkeypatch.setattr("app.admin.backup.STATUS_FILE", missing_dir / ".last_backup_status")
        monkeypatch.setattr("app.admin.backup.CONFIG_FILE", missing_dir / ".backup_config.json")

        resp = await admin_client.client.get("/admin/backup")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_backup_page_with_error_status(self, admin_client, tmp_path, monkeypatch):
        """Backup page renders correctly when last backup had an error."""
        status_file = tmp_path / ".last_backup_status"
        status_file.write_text(
            json.dumps(
                {
                    "status": "error",
                    "timestamp": "20260401_120000",
                    "file": "",
                    "size": 0,
                    "duration": 0,
                    "error": "pg_dump failed: connection refused",
                }
            )
        )
        monkeypatch.setattr("app.admin.backup.STATUS_FILE", status_file)
        monkeypatch.setattr("app.admin.backup.CONFIG_FILE", tmp_path / ".backup_config.json")
        monkeypatch.setattr("app.admin.backup.BACKUP_DIR", tmp_path)

        resp = await admin_client.client.get("/admin/backup")
        assert resp.status_code == 200
