"""Tests for logging plugin discovery and GCP severity processor."""

import json
import io
import logging
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from almanak.framework.utils.logging import (
    _load_plugin_processors,
    configure_logging,
    LogFormat,
    LogLevel,
)

# Make platform-plugins importable for tests without installing the package
_PLATFORM_PLUGINS_DIR = str(Path(__file__).resolve().parents[2] / "platform-plugins")
if _PLATFORM_PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLATFORM_PLUGINS_DIR)

from almanak_platform.gcp_logging import _add_gcp_severity, get_processors  # noqa: E402


# ---------------------------------------------------------------------------
# Plugin discovery tests
# ---------------------------------------------------------------------------


class TestLoadPluginProcessors:
    """Tests for _load_plugin_processors entry point discovery."""

    def test_returns_empty_when_no_plugins(self):
        with patch("importlib.metadata.entry_points", return_value=[]):
            result = _load_plugin_processors()
        assert result == []

    def test_loads_all_matching_entry_points(self):
        """All registered plugins are loaded, not just the first."""
        proc_a = MagicMock(name="proc_a")
        proc_b = MagicMock(name="proc_b")

        ep1 = MagicMock()
        ep1.load.return_value = lambda: [proc_a]
        ep1.value = "pkg_a.mod:fn"

        ep2 = MagicMock()
        ep2.load.return_value = lambda: [proc_b]
        ep2.value = "pkg_b.mod:fn"

        with patch("importlib.metadata.entry_points", return_value=[ep1, ep2]):
            result = _load_plugin_processors()

        assert result == [proc_a, proc_b]

    def test_warns_on_broken_plugin_and_continues(self, caplog):
        """A broken plugin logs a warning and doesn't block other plugins."""
        proc_good = MagicMock(name="proc_good")

        ep_bad = MagicMock()
        ep_bad.load.side_effect = ImportError("missing dep")
        ep_bad.value = "broken_pkg.mod:fn"

        ep_good = MagicMock()
        ep_good.load.return_value = lambda: [proc_good]
        ep_good.value = "good_pkg.mod:fn"

        with patch("importlib.metadata.entry_points", return_value=[ep_bad, ep_good]):
            with caplog.at_level(logging.WARNING):
                result = _load_plugin_processors()

        assert result == [proc_good]
        assert "Failed to load almanak.logging plugin" in caplog.text
        assert "broken_pkg.mod:fn" in caplog.text


# ---------------------------------------------------------------------------
# GCP severity processor tests
# ---------------------------------------------------------------------------


class TestGcpSeverityProcessor:
    """Tests for the GCP severity structlog processor."""

    @pytest.mark.parametrize(
        "level, expected_severity",
        [
            ("debug", "DEBUG"),
            ("info", "INFO"),
            ("warning", "WARNING"),
            ("error", "ERROR"),
            ("critical", "CRITICAL"),
            ("exception", "ERROR"),
        ],
    )
    def test_severity_values(self, level, expected_severity):
        event_dict = {"level": level}
        result = _add_gcp_severity(None, level, event_dict)
        assert result["severity"] == expected_severity

    def test_unknown_level_defaults_to_default(self):
        event_dict = {"level": "custom_level"}
        result = _add_gcp_severity(None, "custom_level", event_dict)
        assert result["severity"] == "DEFAULT"

    def test_preserves_level_field(self):
        event_dict = {"level": "error", "message": "boom"}
        result = _add_gcp_severity(None, "error", event_dict)
        assert result["level"] == "error"
        assert result["severity"] == "ERROR"


# ---------------------------------------------------------------------------
# Integration: severity appears in JSON output
# ---------------------------------------------------------------------------


class TestGcpSeverityIntegration:
    """Test that severity field appears in JSON log output when plugin is active."""

    def test_severity_in_json_output(self):
        stream = io.StringIO()

        with patch(
            "almanak.framework.utils.logging._load_plugin_processors",
            return_value=get_processors(),
        ):
            configure_logging(level=LogLevel.INFO, format=LogFormat.JSON, stream=stream)

        test_logger = logging.getLogger("test.gcp_severity")
        test_logger.info("test message")

        output = stream.getvalue().strip()
        log_entry = json.loads(output)
        assert log_entry["severity"] == "INFO"
        assert log_entry["level"] == "info"

    def test_error_severity_in_json_output(self):
        stream = io.StringIO()

        with patch(
            "almanak.framework.utils.logging._load_plugin_processors",
            return_value=get_processors(),
        ):
            configure_logging(level=LogLevel.INFO, format=LogFormat.JSON, stream=stream)

        test_logger = logging.getLogger("test.gcp_severity_error")
        test_logger.error("something broke")

        output = stream.getvalue().strip()
        log_entry = json.loads(output)
        assert log_entry["severity"] == "ERROR"
        assert log_entry["level"] == "error"
