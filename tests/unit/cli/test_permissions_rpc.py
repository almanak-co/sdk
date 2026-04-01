"""Unit tests for RPC URL resolution in the permissions CLI."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from almanak.framework.cli.permissions import _resolve_rpc_url


class TestResolveRpcUrl:
    """Tests for _resolve_rpc_url()."""

    def test_explicit_url_takes_precedence(self) -> None:
        """Explicit --rpc-url overrides ALCHEMY_API_KEY env."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "some-key"}):
            result = _resolve_rpc_url("https://custom-rpc.example.com", "base")
        assert result == "https://custom-rpc.example.com"

    def test_alchemy_key_resolves_per_chain(self) -> None:
        """ALCHEMY_API_KEY env builds the correct per-chain URL."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-key-123"}, clear=False):
            base_url = _resolve_rpc_url(None, "base")
            arb_url = _resolve_rpc_url(None, "arbitrum")
            eth_url = _resolve_rpc_url(None, "ethereum")

        assert base_url == "https://base-mainnet.g.alchemy.com/v2/test-key-123"
        assert arb_url == "https://arb-mainnet.g.alchemy.com/v2/test-key-123"
        assert eth_url == "https://eth-mainnet.g.alchemy.com/v2/test-key-123"

    def test_unsupported_chain_returns_none(self) -> None:
        """Unsupported chain with ALCHEMY_API_KEY returns None."""
        with patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-key"}, clear=False):
            result = _resolve_rpc_url(None, "solana")
        assert result is None

    def test_no_alchemy_key_returns_none(self) -> None:
        """No ALCHEMY_API_KEY and no explicit URL returns None."""
        env = {k: v for k, v in os.environ.items() if k != "ALCHEMY_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            result = _resolve_rpc_url(None, "base")
        assert result is None

    def test_explicit_url_ignores_chain(self) -> None:
        """Explicit URL is returned as-is regardless of chain."""
        result = _resolve_rpc_url("https://my-rpc.io", "unknown_chain_xyz")
        assert result == "https://my-rpc.io"


def test_resolved_rpc_url_forwarded_to_generate_manifest(tmp_path: object) -> None:
    """Resolved RPC URL is passed as rpc_url kwarg to generate_manifest."""
    from pathlib import Path

    tmp = Path(str(tmp_path))

    # Create minimal strategy.py so the CLI doesn't exit early
    (tmp / "strategy.py").write_text("class Stub: pass")

    mock_manifest = MagicMock()
    mock_manifest.permissions = []
    mock_manifest.warnings = []
    mock_manifest.to_zodiac_targets.return_value = []

    metadata = MagicMock(
        name="test_strat",
        supported_protocols=["aerodrome"],
        intent_types=["SWAP"],
        supported_chains=["base"],
        default_chain="base",
    )
    mock_class = MagicMock(STRATEGY_METADATA=metadata)

    with (
        patch.dict(os.environ, {"ALCHEMY_API_KEY": "test-key-forwarding"}, clear=False),
        patch(
            "almanak.framework.cli.permissions.load_strategy_from_file",
            return_value=(mock_class, None),
        ),
        patch("almanak.framework.permissions.generator.load_strategy_config", return_value={}),
        patch("almanak.framework.permissions.generator.discover_teardown_protocols", return_value=(set(), [])),
        patch("almanak.framework.permissions.generator.generate_manifest", return_value=mock_manifest) as mock_gen,
    ):
        from click.testing import CliRunner

        from almanak.framework.cli.permissions import permissions

        runner = CliRunner()
        runner.invoke(permissions, ["--working-dir", str(tmp)], catch_exceptions=False)

        # Verify generate_manifest was called with the resolved RPC URL
        assert mock_gen.called
        _, kwargs = mock_gen.call_args
        assert "rpc_url" in kwargs
        assert kwargs["rpc_url"] is not None
        assert "test-key-forwarding" in kwargs["rpc_url"]
