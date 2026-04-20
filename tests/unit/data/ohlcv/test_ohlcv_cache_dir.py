"""Tests for OHLCV router cache directory resolution."""

from pathlib import Path
from unittest.mock import patch

from almanak.framework.data.ohlcv.ohlcv_router import _resolve_cache_dir


def test_resolve_cache_dir_uses_home(tmp_path: Path) -> None:
    """When home dir is writable, cache goes under ~/.almanak/."""
    with patch("almanak.framework.data.ohlcv.ohlcv_router.Path.home", return_value=tmp_path):
        result = _resolve_cache_dir()
    assert result == tmp_path / ".almanak" / "data_cache" / "ohlcv"
    assert result.exists()


def test_resolve_cache_dir_falls_back_on_oserror(tmp_path: Path) -> None:
    """When home dir mkdir raises OSError, falls back to system temp dir."""
    original_mkdir = Path.mkdir

    def failing_mkdir(self: Path, *args: object, **kwargs: object) -> None:
        if "no_write" in str(self):
            raise OSError("Permission denied")
        original_mkdir(self, *args, **kwargs)

    fake_home = tmp_path / "no_write"

    with (
        patch("almanak.framework.data.ohlcv.ohlcv_router.Path.home", return_value=fake_home),
        patch("almanak.framework.data.ohlcv.ohlcv_router.tempfile.gettempdir", return_value=str(tmp_path / "tmp")),
        patch.object(Path, "mkdir", failing_mkdir),
    ):
        result = _resolve_cache_dir()

    assert "data_cache" in str(result)
    assert str(tmp_path / "tmp") in str(result)
