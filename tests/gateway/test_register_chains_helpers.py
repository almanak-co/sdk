"""Unit tests for phase helpers extracted from ``RegisterChains`` (Phase 8.3c).

These tests are *unit-level*: each helper is exercised in isolation with
lightweight fakes for the wallet registry and execution servicer. They
complement the RPC-level characterization tests in
``test_register_chains_characterization.py`` by pinning helper-module
contracts directly, so later refactors of the RPC wiring do not mask bugs
inside the helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.gateway._register_chains_helpers import (
    derive_default_wallet,
    find_solana_chain_in_wallets,
    merge_all_registry_chains,
    prewarm_chains,
    reinitialize_market_service,
    resolve_requested_chain_wallets,
    validate_and_map_chains,
)
from almanak.gateway.core.settings import GatewaySettings

TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_EOA_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_SAFE_ADDRESS = "0xSafe0000000000000000000000000000000000AA"


@dataclass
class _FakeResolved:
    account_address: str
    family: str = "evm"


def _settings(**kwargs) -> GatewaySettings:
    defaults = {
        "metrics_enabled": False,
        "audit_enabled": False,
    }
    defaults.update(kwargs)
    return GatewaySettings(**defaults)


def _null_settings(monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    monkeypatch.delenv("ALMANAK_GATEWAY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)
    s = _settings(private_key=None, safe_address=None, safe_mode=None)
    s.private_key = None
    return s


# ---------------------------------------------------------------------------
# derive_default_wallet
# ---------------------------------------------------------------------------
class TestDeriveDefaultWallet:
    def test_request_wallet_wins(self) -> None:
        s = _settings(safe_address=TEST_SAFE_ADDRESS, safe_mode="direct", private_key=TEST_PRIVATE_KEY)
        assert derive_default_wallet(s, "0xFromRequest") == "0xFromRequest"

    def test_safe_direct_mode(self) -> None:
        s = _settings(safe_address=TEST_SAFE_ADDRESS, safe_mode="direct", private_key=TEST_PRIVATE_KEY)
        assert derive_default_wallet(s, "") == TEST_SAFE_ADDRESS

    def test_safe_zodiac_mode(self) -> None:
        s = _settings(safe_address=TEST_SAFE_ADDRESS, safe_mode="zodiac", private_key=TEST_PRIVATE_KEY)
        assert derive_default_wallet(s, "") == TEST_SAFE_ADDRESS

    def test_safe_address_without_mode_falls_back_to_eoa(self) -> None:
        """safe_address configured but safe_mode=None must not use Safe."""
        s = _settings(safe_address=TEST_SAFE_ADDRESS, safe_mode=None, private_key=TEST_PRIVATE_KEY)
        result = derive_default_wallet(s, "")
        assert result.lower() == TEST_EOA_ADDRESS.lower()

    def test_invalid_safe_mode_falls_back_to_eoa(self) -> None:
        """safe_mode with a non-enabled value also falls back to EOA."""
        s = _settings(safe_address=TEST_SAFE_ADDRESS, safe_mode="disabled", private_key=TEST_PRIVATE_KEY)
        result = derive_default_wallet(s, "")
        assert result.lower() == TEST_EOA_ADDRESS.lower()

    def test_private_key_with_0x_prefix(self) -> None:
        s = _settings(safe_address=None, safe_mode=None, private_key=TEST_PRIVATE_KEY)
        result = derive_default_wallet(s, "")
        assert result.lower() == TEST_EOA_ADDRESS.lower()

    def test_private_key_without_0x_prefix(self) -> None:
        bare = TEST_PRIVATE_KEY[2:]
        s = _settings(safe_address=None, safe_mode=None, private_key=bare)
        result = derive_default_wallet(s, "")
        assert result.lower() == TEST_EOA_ADDRESS.lower()

    def test_no_wallet_anywhere_returns_empty_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        s = _null_settings(monkeypatch)
        assert derive_default_wallet(s, "") == ""

    def test_malformed_private_key_returns_empty_instead_of_raising(self, caplog: pytest.LogCaptureFixture) -> None:
        """A bad gateway private key must not escape as gRPC INTERNAL.

        The helper logs a warning and returns "" so ``RegisterChains`` can
        surface the canonical wallet-missing error-response instead.
        """
        s = _settings(safe_address=None, safe_mode=None, private_key="not_a_valid_key")
        import logging

        with caplog.at_level(logging.WARNING, logger="almanak.gateway._register_chains_helpers"):
            result = derive_default_wallet(s, "")
        assert result == ""
        assert any("Invalid gateway private key" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# resolve_requested_chain_wallets
# ---------------------------------------------------------------------------
class TestResolveRequestedChainWallets:
    def test_no_registry_returns_empty_dict(self) -> None:
        assert resolve_requested_chain_wallets(None, ["arbitrum"]) == {}

    def test_registry_resolves_each_chain(self) -> None:
        registry = MagicMock()
        registry.resolve.side_effect = lambda c: _FakeResolved(account_address=f"0xwallet_{c}")
        result = resolve_requested_chain_wallets(registry, ["arbitrum", "base"])
        assert result == {"arbitrum": "0xwallet_arbitrum", "base": "0xwallet_base"}

    def test_registry_skips_solana_family(self) -> None:
        registry = MagicMock()

        def resolve(c):
            family = "solana" if c == "arbitrum" else "evm"
            return _FakeResolved(account_address=f"0xwallet_{c}", family=family)

        registry.resolve.side_effect = resolve
        result = resolve_requested_chain_wallets(registry, ["arbitrum", "base"])
        # 'arbitrum' came back as solana family -> skipped
        assert result == {"base": "0xwallet_base"}

    def test_invalid_chain_swallowed(self) -> None:
        registry = MagicMock()
        registry.resolve.side_effect = lambda c: _FakeResolved(account_address="0xok")
        # validate_chain will raise for a made-up name; we expect it silently skipped.
        result = resolve_requested_chain_wallets(registry, ["not_a_chain_xyz"])
        assert result == {}

    def test_registry_resolve_exception_swallowed(self) -> None:
        registry = MagicMock()
        registry.resolve.side_effect = RuntimeError("boom")
        result = resolve_requested_chain_wallets(registry, ["arbitrum"])
        assert result == {}


# ---------------------------------------------------------------------------
# find_solana_chain_in_wallets
# ---------------------------------------------------------------------------
class TestFindSolanaChainInWallets:
    def test_no_solana_returns_none(self) -> None:
        assert find_solana_chain_in_wallets(["arbitrum"], {"arbitrum": "0x"}) is None

    def test_solana_in_wallets_returns_chain(self) -> None:
        assert find_solana_chain_in_wallets(["solana"], {"solana": "0x"}) == "solana"

    def test_solana_requested_but_not_in_wallets_returns_none(self) -> None:
        """If registry dropped solana, the guard does not fire."""
        assert find_solana_chain_in_wallets(["solana"], {}) is None

    def test_case_insensitive(self) -> None:
        assert find_solana_chain_in_wallets(["SOLANA"], {"solana": "0x"}) == "SOLANA"

    def test_returns_first_match_in_request_order(self) -> None:
        """Ordering is stable with the request order so error messages are deterministic."""
        chains = ["arbitrum", "solana"]
        assert find_solana_chain_in_wallets(chains, {"solana": "0x", "arbitrum": "0x"}) == "solana"


# ---------------------------------------------------------------------------
# validate_and_map_chains
# ---------------------------------------------------------------------------
class TestValidateAndMapChains:
    def test_happy_path_uses_registry_wallet(self) -> None:
        chain_wallets = {"arbitrum": "0xRegistry"}
        result, errors = validate_and_map_chains(["arbitrum"], chain_wallets, legacy_wallet="0xLegacy")
        assert result == {"arbitrum": "0xRegistry"}
        assert errors == []

    def test_falls_back_to_legacy_wallet(self) -> None:
        result, errors = validate_and_map_chains(["arbitrum"], {}, legacy_wallet="0xLegacy")
        assert result == {"arbitrum": "0xLegacy"}
        assert errors == []

    def test_invalid_chain_records_error(self) -> None:
        result, errors = validate_and_map_chains(["not_a_real_chain_xyz"], {}, legacy_wallet="0xLegacy")
        assert result == {}
        assert len(errors) == 1
        assert "not_a_real_chain_xyz" in errors[0]

    def test_missing_wallet_records_error(self) -> None:
        """Valid chain but no registry entry and empty legacy wallet -> error collected."""
        result, errors = validate_and_map_chains(["arbitrum"], {}, legacy_wallet="")
        assert result == {}
        assert errors == ["arbitrum: No wallet address available"]

    def test_mixed_good_and_bad_chains(self) -> None:
        result, errors = validate_and_map_chains(
            ["arbitrum", "not_a_chain", "base"],
            {"base": "0xBase"},
            legacy_wallet="0xLegacy",
        )
        assert result == {"arbitrum": "0xLegacy", "base": "0xBase"}
        assert len(errors) == 1
        assert "not_a_chain" in errors[0]

    def test_empty_chains_returns_empty_map(self) -> None:
        result, errors = validate_and_map_chains([], {}, legacy_wallet="0xAny")
        assert result == {}
        assert errors == []


# ---------------------------------------------------------------------------
# merge_all_registry_chains
# ---------------------------------------------------------------------------
class TestMergeAllRegistryChains:
    def test_no_registry_returns_copy(self) -> None:
        existing = {"arbitrum": "0xA"}
        result = merge_all_registry_chains(None, existing)
        assert result == existing
        # Must be a copy - caller should not mutate the original by accident.
        assert result is not existing

    def test_adds_non_requested_registry_chains(self) -> None:
        registry = MagicMock()
        registry.all_chains.return_value = ["arbitrum", "base"]
        registry.resolve.side_effect = lambda c: _FakeResolved(account_address=f"0x{c}")
        result = merge_all_registry_chains(registry, {"arbitrum": "0xExisting"})
        # existing entry preserved (not overwritten by registry)
        assert result["arbitrum"] == "0xExisting"
        # unrequested chain added
        assert result["base"] == "0xbase"

    def test_skips_solana_registry_entries(self) -> None:
        registry = MagicMock()
        registry.all_chains.return_value = ["base", "solana"]

        def resolve(c):
            fam = "solana" if c == "solana" else "evm"
            return _FakeResolved(account_address=f"0x{c}", family=fam)

        registry.resolve.side_effect = resolve
        result = merge_all_registry_chains(registry, {})
        assert result == {"base": "0xbase"}

    def test_swallows_resolve_exceptions(self) -> None:
        registry = MagicMock()
        registry.all_chains.return_value = ["base", "broken"]

        def resolve(c):
            if c == "broken":
                raise RuntimeError("boom")
            return _FakeResolved(account_address=f"0x{c}")

        registry.resolve.side_effect = resolve
        result = merge_all_registry_chains(registry, {})
        assert result == {"base": "0xbase"}

    def test_does_not_mutate_input_map(self) -> None:
        registry = MagicMock()
        registry.all_chains.return_value = ["base"]
        registry.resolve.side_effect = lambda c: _FakeResolved(account_address="0xbase")
        existing = {"arbitrum": "0xA"}
        merge_all_registry_chains(registry, existing)
        assert existing == {"arbitrum": "0xA"}  # unchanged


# ---------------------------------------------------------------------------
# prewarm_chains
# ---------------------------------------------------------------------------
class TestPrewarmChains:
    @pytest.mark.asyncio
    async def test_all_success(self) -> None:
        execution = MagicMock()
        execution._get_orchestrator = AsyncMock()
        execution._get_compiler = MagicMock()
        initialized, errors = await prewarm_chains(execution, {"arbitrum": "0xA", "base": "0xB"})
        assert initialized == ["arbitrum", "base"]
        assert errors == []
        assert execution._get_orchestrator.await_count == 2
        assert execution._get_compiler.call_count == 2

    @pytest.mark.asyncio
    async def test_orchestrator_exception_collected(self) -> None:
        execution = MagicMock()
        execution._get_orchestrator = AsyncMock(side_effect=RuntimeError("rpc down"))
        execution._get_compiler = MagicMock()
        initialized, errors = await prewarm_chains(execution, {"arbitrum": "0xA"})
        assert initialized == []
        assert errors == ["arbitrum: rpc down"]
        execution._get_compiler.assert_not_called()

    @pytest.mark.asyncio
    async def test_compiler_exception_collected(self) -> None:
        execution = MagicMock()
        execution._get_orchestrator = AsyncMock()
        execution._get_compiler = MagicMock(side_effect=ValueError("bad config"))
        initialized, errors = await prewarm_chains(execution, {"arbitrum": "0xA"})
        assert initialized == []
        assert errors == ["arbitrum: bad config"]

    @pytest.mark.asyncio
    async def test_partial_failure_continues_to_next_chain(self) -> None:
        execution = MagicMock()

        async def get_orch(chain, wallet):
            if chain == "base":
                raise RuntimeError("down")

        execution._get_orchestrator = AsyncMock(side_effect=get_orch)
        execution._get_compiler = MagicMock()
        initialized, errors = await prewarm_chains(execution, {"arbitrum": "0xA", "base": "0xB", "ethereum": "0xE"})
        assert initialized == ["arbitrum", "ethereum"]
        assert len(errors) == 1
        assert "base" in errors[0]

    @pytest.mark.asyncio
    async def test_empty_map_is_no_op(self) -> None:
        execution = MagicMock()
        execution._get_orchestrator = AsyncMock()
        execution._get_compiler = MagicMock()
        initialized, errors = await prewarm_chains(execution, {})
        assert initialized == []
        assert errors == []
        execution._get_orchestrator.assert_not_awaited()


# ---------------------------------------------------------------------------
# reinitialize_market_service
# ---------------------------------------------------------------------------
class TestReinitializeMarketService:
    @pytest.mark.asyncio
    async def test_calls_reinit_with_first_chain(self) -> None:
        market = MagicMock()
        market.reinitialize = AsyncMock()
        await reinitialize_market_service(market, ["arbitrum", "base"])
        market.reinitialize.assert_awaited_once_with("arbitrum")

    @pytest.mark.asyncio
    async def test_noop_when_no_market_servicer(self) -> None:
        # Must not raise
        await reinitialize_market_service(None, ["arbitrum"])

    @pytest.mark.asyncio
    async def test_noop_when_no_initialized_chains(self) -> None:
        market = MagicMock()
        market.reinitialize = AsyncMock()
        await reinitialize_market_service(market, [])
        market.reinitialize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_swallows_reinit_exception(self) -> None:
        market = MagicMock()
        market.reinitialize = AsyncMock(side_effect=RuntimeError("boom"))
        # Must not raise
        await reinitialize_market_service(market, ["arbitrum"])
        market.reinitialize.assert_awaited_once()
