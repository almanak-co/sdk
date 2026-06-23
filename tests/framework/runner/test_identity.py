"""Tests for the deployment_id resolver (blueprint 29, VIB-4722).

Validates:
- Hosted: deployment_id is ALMANAK_DEPLOYMENT_ID verbatim; blank ⇒ FatalBootError
- Local: deterministic deployment:{sha256(wallet:chain)[:12]} (class name NOT hashed)
- Local with no wallet/chain ⇒ FatalBootError (no bare-name fallback, no --id)
- run_id is always unique
"""

import hashlib

import pytest

from almanak.framework.deployment import FatalBootError
from almanak.framework.runner.identity import generate_run_id, resolve_deployment_id


def _local_env(monkeypatch):
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)


class TestResolveDeploymentIdLocal:
    """Local mode: deployment_id is a pure function of (wallet, chain)."""

    def test_deterministic_hash(self, monkeypatch):
        """Same (wallet, chain) always produces the same deployment_id."""
        _local_env(monkeypatch)
        id1 = resolve_deployment_id(wallet_address="0xAbC123", chain="arbitrum")
        id2 = resolve_deployment_id(wallet_address="0xAbC123", chain="arbitrum")
        assert id1 == id2
        assert id1.startswith("deployment:")
        assert len(id1.split(":")[1]) == 12

    def test_hash_matches_blueprint_formula(self, monkeypatch):
        """deployment_id == deployment:{sha256(wallet:chain)[:12]}, lowercased."""
        _local_env(monkeypatch)
        result = resolve_deployment_id(wallet_address="0xAbC123", chain="Arbitrum")
        key = "0xabc123:arbitrum"
        expected = f"deployment:{hashlib.sha256(key.encode()).hexdigest()[:12]}"
        assert result == expected

    def test_case_insensitive_wallet_and_chain(self, monkeypatch):
        """Wallet address and chain are lowercased before hashing."""
        _local_env(monkeypatch)
        id1 = resolve_deployment_id(wallet_address="0xABCDEF", chain="Arbitrum")
        id2 = resolve_deployment_id(wallet_address="0xabcdef", chain="arbitrum")
        assert id1 == id2

    def test_different_wallets_different_ids(self, monkeypatch):
        _local_env(monkeypatch)
        id1 = resolve_deployment_id(wallet_address="0xAAA", chain="arbitrum")
        id2 = resolve_deployment_id(wallet_address="0xBBB", chain="arbitrum")
        assert id1 != id2

    def test_different_chains_different_ids(self, monkeypatch):
        _local_env(monkeypatch)
        id1 = resolve_deployment_id(wallet_address="0xAAA", chain="arbitrum")
        id2 = resolve_deployment_id(wallet_address="0xAAA", chain="base")
        assert id1 != id2

    def test_caip2_not_silently_wired_into_deployment_id(self, monkeypatch):
        """CAIP-2 adoption (VIB-5175) must NOT change the deployment_id hash input.

        The hash folds the chain string verbatim, so the canonical name is
        still hashed as ``"arbitrum"`` (not ``"eip155:42161"``). Feeding a
        CAIP-2 string here would re-fork every existing local deployment's
        identity — that is explicitly out of CAIP Phase-1 scope. This guard
        fails loudly if a future change normalizes the chain to CAIP-2 before
        hashing.
        """
        _local_env(monkeypatch)
        canonical = resolve_deployment_id(wallet_address="0xAAA", chain="arbitrum")
        key = "0xaaa:arbitrum"
        assert canonical == f"deployment:{hashlib.sha256(key.encode()).hexdigest()[:12]}"
        # A CAIP-2 chain string is hashed verbatim (treated as a different
        # string), proving the resolver does not canonicalize to CAIP-2.
        caip = resolve_deployment_id(wallet_address="0xAAA", chain="eip155:42161")
        assert caip != canonical

    def test_no_wallet_raises_fatal(self, monkeypatch):
        """No wallet ⇒ FatalBootError (no bare-name fallback)."""
        _local_env(monkeypatch)
        with pytest.raises(FatalBootError):
            resolve_deployment_id(chain="arbitrum")

    def test_empty_wallet_raises_fatal(self, monkeypatch):
        """Empty wallet ⇒ FatalBootError."""
        _local_env(monkeypatch)
        with pytest.raises(FatalBootError):
            resolve_deployment_id(wallet_address="", chain="arbitrum")

    def test_no_chain_raises_fatal(self, monkeypatch):
        """No chain ⇒ FatalBootError."""
        _local_env(monkeypatch)
        with pytest.raises(FatalBootError):
            resolve_deployment_id(wallet_address="0xAAA", chain="")

    def test_stray_deployment_id_ignored_in_local(self, monkeypatch):
        """A stray ALMANAK_DEPLOYMENT_ID does not flip a local run to hosted."""
        monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "platform-uuid-9999")
        result = resolve_deployment_id(wallet_address="0xAAA", chain="arbitrum")
        assert result.startswith("deployment:")
        assert result != "platform-uuid-9999"


class TestResolveDeploymentIdHosted:
    """Hosted mode: deployment_id is ALMANAK_DEPLOYMENT_ID verbatim."""

    def test_hosted_uses_deployment_id_env(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "platform-agent-1234")
        # wallet/chain are ignored in hosted mode.
        result = resolve_deployment_id(wallet_address="0xAAA", chain="arbitrum")
        assert result == "platform-agent-1234"

    def test_hosted_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "1")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "  agent-7  ")
        assert resolve_deployment_id() == "agent-7"

    def test_hosted_blank_id_raises_fatal(self, monkeypatch):
        """Hosted with a blank ALMANAK_DEPLOYMENT_ID ⇒ FatalBootError."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "true")
        monkeypatch.setenv("ALMANAK_DEPLOYMENT_ID", "   ")
        with pytest.raises(FatalBootError):
            resolve_deployment_id()

    def test_hosted_unset_id_raises_fatal(self, monkeypatch):
        """Hosted with ALMANAK_DEPLOYMENT_ID unset ⇒ FatalBootError."""
        monkeypatch.setenv("ALMANAK_IS_HOSTED", "yes")
        monkeypatch.delenv("ALMANAK_DEPLOYMENT_ID", raising=False)
        with pytest.raises(FatalBootError):
            resolve_deployment_id()


class TestGenerateRunId:
    """Test per-process run_id generation."""

    def test_run_id_is_12_hex_chars(self):
        rid = generate_run_id()
        assert len(rid) == 12
        int(rid, 16)  # Must be valid hex

    def test_run_ids_are_unique(self):
        ids = {generate_run_id() for _ in range(100)}
        assert len(ids) == 100
