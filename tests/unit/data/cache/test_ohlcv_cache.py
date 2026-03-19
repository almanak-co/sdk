"""Unit tests for OHLCVCache filesystem fallback logic."""

from pathlib import Path
from unittest.mock import patch

from almanak.framework.data.cache.ohlcv_cache import OHLCVCache


class TestOHLCVCacheFilesystemFallback:
    """Tests for OHLCVCache filesystem fallback when home dir is not writable."""

    def test_default_path_uses_home_dir(self, tmp_path):
        """Default (None) resolves to ~/.almanak/cache/ohlcv_cache.db."""
        fake_home = tmp_path / "home"
        expected = str(fake_home / ".almanak" / "cache" / "ohlcv_cache.db")
        with patch.object(Path, "home", return_value=fake_home):
            cache = OHLCVCache()
        assert cache.db_path == expected

    def test_fallback_to_tmp_when_home_not_writable(self):
        """Falls back to /tmp when home directory mkdir raises OSError."""
        original_mkdir = Path.mkdir

        def selective_mkdir(self, *args, **kwargs):
            if ".almanak" in str(self) and "/tmp" not in str(self):
                raise OSError("Read-only file system")
            return original_mkdir(self, *args, **kwargs)

        with patch.object(Path, "mkdir", selective_mkdir):
            cache = OHLCVCache()
        assert "/tmp/.almanak/cache/ohlcv_cache.db" in cache.db_path

    def test_explicit_path_bypasses_fallback(self):
        """Explicit db_path is used directly without fallback."""
        cache = OHLCVCache(db_path=":memory:")
        assert cache.db_path == ":memory:"
