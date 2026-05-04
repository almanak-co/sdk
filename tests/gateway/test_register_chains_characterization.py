"""Characterization tests for ``_RegisterChainsServicer.RegisterChains`` (Phase 8.3c).

These tests capture the current observable behaviour of the RPC as documented
checkpoints. They do not change production code - if any assertion fails, the
refactor broke a behaviour the tests pinned.

Focus areas (complementing ``test_wallet_resolution.py`` which covers only
wallet-resolution happy paths):

- Request parsing (empty chains, explicit wallet_address).
- Default wallet derivation: Safe (direct/zodiac) vs private-key EOA vs none.
- Wallet-missing error path.
- Wallet-registry per-chain resolution: normal, Solana skip (via ``family``),
  and ``resolve()`` exception path.
- Solana reject guard after registry resolution.
- Chain validation errors collected into ``errors`` list.
- Per-chain wallet fallback to legacy ``wallet_address``.
- Missing effective wallet path (chain has no registry entry and no legacy
  wallet).
- ``full_chain_wallets`` merge: includes registry chains that weren't
  requested; Solana registry chains skipped; ``resolve()`` failures swallowed.
- ``_registered_chain_wallets`` assignment behaviour (``None`` when map empty).
- Compiler cache clear (``_compiler_cache.clear()`` always called).
- Pre-warm loop: orchestrator/compiler success, orchestrator exception path.
- ``_registered_chains`` set assignment (post-pre-warm).
- MarketService reinit: called on success, failure is swallowed, skipped when
  no initialized chains or no market servicer.
- Legacy ``wallet_address`` field derivation for response.
- Partial-failure response (some initialized, some errors) - ``success=False``
  with populated fields.
- Success response shape.

All tests use the shared harness in ``grpc_harness``. These are pure
unit tests - no network, no DB, no subprocess.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.server import _RegisterChainsServicer
from tests.gateway.grpc_harness import make_grpc_context

# A well-known test private key (anvil default #0) and its derived address.
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_EOA_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_SAFE_ADDRESS = "0xSafe0000000000000000000000000000000000AA"
EXPLICIT_WALLET = "0xExplicit0000000000000000000000000000AABB"


# ---------------------------------------------------------------------------
# Request stub + helpers
# ---------------------------------------------------------------------------
@dataclass
class _FakeRegisterChainsRequest:
    chains: list[str] = field(default_factory=list)
    wallet_address: str = ""


@dataclass
class _FakeResolvedWallet:
    account_address: str
    family: str = "evm"
    kind: str = "eoa"


def _settings_eoa(**kwargs) -> GatewaySettings:
    defaults = {
        "private_key": TEST_PRIVATE_KEY,
        "safe_address": None,
        "safe_mode": None,
        "metrics_enabled": False,
        "audit_enabled": False,
    }
    defaults.update(kwargs)
    return GatewaySettings(**defaults)


def _settings_safe(**kwargs) -> GatewaySettings:
    defaults = {
        "private_key": TEST_PRIVATE_KEY,
        "safe_address": TEST_SAFE_ADDRESS,
        "safe_mode": "direct",
        "metrics_enabled": False,
        "audit_enabled": False,
    }
    defaults.update(kwargs)
    return GatewaySettings(**defaults)


def _make_servicer(
    settings: GatewaySettings,
    *,
    wallet_registry=None,
    market_servicer=None,
) -> _RegisterChainsServicer:
    health = MagicMock()
    execution = MagicMock()
    # Defaults used almost everywhere: orchestrator is async, compiler sync,
    # plus the cache + registered-chain attributes it writes to.
    execution._get_orchestrator = AsyncMock()
    execution._get_compiler = MagicMock()
    execution._compiler_cache = MagicMock()
    execution._registered_chain_wallets = None
    execution._registered_chains = set()
    return _RegisterChainsServicer(
        health,
        execution,
        settings,
        wallet_registry=wallet_registry,
        market_servicer=market_servicer,
    )


# ---------------------------------------------------------------------------
# Default-wallet derivation (no registry)
# ---------------------------------------------------------------------------
class TestDefaultWalletDerivation:
    @pytest.mark.asyncio
    async def test_explicit_wallet_address_beats_settings(self) -> None:
        servicer = _make_servicer(_settings_safe())
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address=EXPLICIT_WALLET)

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.wallet_address == EXPLICIT_WALLET
        servicer._execution._get_orchestrator.assert_awaited_once_with("arbitrum", EXPLICIT_WALLET)

    @pytest.mark.asyncio
    async def test_safe_direct_mode_uses_safe_address(self) -> None:
        servicer = _make_servicer(_settings_safe(safe_mode="direct"))
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.wallet_address == TEST_SAFE_ADDRESS

    @pytest.mark.asyncio
    async def test_safe_zodiac_mode_uses_safe_address(self) -> None:
        servicer = _make_servicer(_settings_safe(safe_mode="zodiac"))
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.wallet_address == TEST_SAFE_ADDRESS

    @pytest.mark.asyncio
    async def test_safe_address_without_safe_mode_falls_back_to_eoa(self) -> None:
        """safe_address set but safe_mode=None -> Safe NOT used, derive EOA."""
        settings = GatewaySettings(
            private_key=TEST_PRIVATE_KEY,
            safe_address=TEST_SAFE_ADDRESS,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        servicer = _make_servicer(settings)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.wallet_address.lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_private_key_without_0x_prefix_derives_eoa(self) -> None:
        """Private key without 0x prefix still produces EOA address."""
        bare_key = TEST_PRIVATE_KEY[2:]  # strip 0x
        settings = GatewaySettings(
            private_key=bare_key,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        servicer = _make_servicer(settings)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.wallet_address.lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_missing_wallet_returns_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No safe_address, no private_key, no wallet_registry -> error string pinned."""
        monkeypatch.delenv("ALMANAK_GATEWAY_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        settings = GatewaySettings(
            private_key=None,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        settings.private_key = None
        servicer = _make_servicer(settings)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        # Pin observable error-response shape byte-for-byte.
        assert response.success is False
        assert response.error == "No wallet_address provided and no private key configured in gateway"
        # Orchestrator pre-warm must NOT run on this early return.
        servicer._execution._get_orchestrator.assert_not_awaited()


# ---------------------------------------------------------------------------
# Wallet registry branches
# ---------------------------------------------------------------------------
class TestWalletRegistryBranch:
    @pytest.mark.asyncio
    async def test_registry_resolves_per_chain_wallets(self) -> None:
        registry = MagicMock()
        registry.resolve.side_effect = lambda c: _FakeResolvedWallet(account_address=f"0xwallet_{c}", family="evm")
        registry.all_chains.return_value = ["arbitrum"]

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert dict(response.chain_wallets) == {"arbitrum": "0xwallet_arbitrum"}
        # orchestrator called with registry-resolved wallet, NOT legacy EOA
        servicer._execution._get_orchestrator.assert_awaited_once_with("arbitrum", "0xwallet_arbitrum")

    @pytest.mark.asyncio
    async def test_registry_skips_solana_via_family_attribute(self) -> None:
        """Resolved entries with ``family='solana'`` are skipped from chain_wallets.

        Registers a chain whose registry entry comes back as Solana family. The
        first-loop skip must drop it from ``chain_wallets`` (so no registry
        wallet is used), and ``validate_and_map_chains`` then has no registry
        entry and falls back to the legacy EOA wallet.
        """
        registry = MagicMock()
        # Every resolved chain returns Solana family - exercises the skip branch.
        registry.resolve.side_effect = lambda c: _FakeResolvedWallet(
            account_address=f"0xwallet_{c}",
            family="solana",
        )
        registry.all_chains.return_value = []

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        # Request a valid EVM chain whose registry entry is (mis)tagged Solana.
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        # Registry entry was skipped -> legacy wallet fallback is used.
        assert response.success is True
        wallets = dict(response.chain_wallets)
        # arbitrum is present, but mapped to the legacy EOA wallet (NOT the
        # Solana-flagged registry wallet).
        assert wallets["arbitrum"] != "0xwallet_arbitrum"
        assert wallets["arbitrum"].lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_registry_resolve_exception_is_swallowed(self) -> None:
        """If registry.resolve raises for a requested chain, we log debug and
        fall back to the legacy wallet path for that chain."""
        registry = MagicMock()

        def resolve_side_effect(chain):
            if chain == "arbitrum":
                raise RuntimeError("boom")
            return _FakeResolvedWallet(account_address=f"0xwallet_{chain}")

        registry.resolve.side_effect = resolve_side_effect
        registry.all_chains.return_value = []

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        # Per-chain registry failed -> falls back to legacy EOA wallet
        assert response.success is True
        called_addr = servicer._execution._get_orchestrator.call_args[0][1]
        assert called_addr.lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_solana_chain_with_registry_entry_rejected(self, monkeypatch) -> None:
        """If a Solana chain name ends up in chain_wallets (via some path),
        the explicit guard (lines 120-128) returns an error response.

        We exercise this by patching ``validate_chain`` to pass ``solana`` through
        unchanged and making the registry's first-loop resolve return an EVM family
        (so chain_wallets gets populated for 'solana'). The second Solana guard
        then checks ``is_solana_chain(chain) and chain.lower() in chain_wallets``.
        """
        import almanak.gateway.validation as vmod

        # Passthrough stub - does not need to route through the normalizer.
        def _passthrough(chain, field="chain"):
            if not chain:
                raise vmod.ValidationError(field, "required")
            return chain.lower().strip()

        monkeypatch.setattr(vmod, "validate_chain", _passthrough)

        registry = MagicMock()
        # Return EVM-family so the FIRST loop writes 'solana' into chain_wallets.
        registry.resolve.side_effect = lambda c: _FakeResolvedWallet(account_address=f"0xwallet_{c}", family="evm")
        registry.all_chains.return_value = []

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["solana"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is False
        assert "Solana" in response.error or "solana" in response.error

    @pytest.mark.asyncio
    async def test_full_chain_wallets_includes_unrequested_registry_chains(self) -> None:
        """``full_chain_wallets`` must include registry chains not in the request."""
        registry = MagicMock()
        registry.resolve.side_effect = lambda c: _FakeResolvedWallet(account_address=f"0xwallet_{c}")
        registry.all_chains.return_value = ["arbitrum", "base", "ethereum"]

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        wallets = dict(response.chain_wallets)
        # Requested chain + the two extra registry chains
        assert wallets == {
            "arbitrum": "0xwallet_arbitrum",
            "base": "0xwallet_base",
            "ethereum": "0xwallet_ethereum",
        }

    @pytest.mark.asyncio
    async def test_full_chain_wallets_skips_solana_registry_entries(self) -> None:
        """Registry chains with family=solana must be skipped from full map."""
        registry = MagicMock()

        def resolve(c):
            return _FakeResolvedWallet(
                account_address=f"0xwallet_{c}",
                family="solana" if c == "solana" else "evm",
            )

        registry.resolve.side_effect = resolve
        registry.all_chains.return_value = ["arbitrum", "solana"]

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert "solana" not in dict(response.chain_wallets)

    @pytest.mark.asyncio
    async def test_full_chain_wallets_swallows_registry_resolve_exception(self) -> None:
        """If ``resolve`` raises during the full-map merge, entry is skipped silently."""
        registry = MagicMock()

        def resolve(c):
            if c == "base":
                raise RuntimeError("transient")
            return _FakeResolvedWallet(account_address=f"0xwallet_{c}")

        registry.resolve.side_effect = resolve
        registry.all_chains.return_value = ["arbitrum", "base"]

        settings = _settings_eoa()
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        wallets = dict(response.chain_wallets)
        assert "arbitrum" in wallets
        assert "base" not in wallets


# ---------------------------------------------------------------------------
# Chain validation + pre-warm
# ---------------------------------------------------------------------------
class TestChainValidationAndPrewarm:
    @pytest.mark.asyncio
    async def test_invalid_chain_collected_as_error(self) -> None:
        servicer = _make_servicer(_settings_eoa())
        request = _FakeRegisterChainsRequest(chains=["arbitrum", "not_a_real_chain_xyz"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is False
        assert "not_a_real_chain_xyz" in response.error
        # Good chain still initialized
        assert "arbitrum" in list(response.initialized_chains)

    @pytest.mark.asyncio
    async def test_orchestrator_exception_collected_as_error(self) -> None:
        servicer = _make_servicer(_settings_eoa())
        servicer._execution._get_orchestrator.side_effect = RuntimeError("rpc down")
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is False
        assert "arbitrum" in response.error
        assert "rpc down" in response.error
        # initialized list empty on full failure
        assert list(response.initialized_chains) == []

    @pytest.mark.asyncio
    async def test_partial_success_returns_failure_with_all_fields(self) -> None:
        """When some chains succeed and others fail, success=False but all fields populated."""
        servicer = _make_servicer(_settings_eoa())

        async def get_orch(chain, wallet):
            if chain == "base":
                raise RuntimeError("kaboom")

        servicer._execution._get_orchestrator.side_effect = get_orch
        request = _FakeRegisterChainsRequest(chains=["arbitrum", "base"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is False
        assert list(response.initialized_chains) == ["arbitrum"]
        assert "base" in response.error
        # Legacy wallet_address still populated on partial failure
        assert response.wallet_address.lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_compiler_cache_cleared(self) -> None:
        """``_compiler_cache.clear()`` is always invoked before pre-warm."""
        servicer = _make_servicer(_settings_eoa())
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        await servicer.RegisterChains(request, make_grpc_context())

        servicer._execution._compiler_cache.clear.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_registered_chain_wallets_set_on_success(self) -> None:
        servicer = _make_servicer(_settings_eoa())
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        await servicer.RegisterChains(request, make_grpc_context())

        # Map populated with the legacy-EOA wallet (no registry is configured
        # in this fixture). ``eth_account`` may return the address in either
        # checksum or lowercase form depending on the key path, so normalize
        # for comparison while still pinning the specific value.
        assert servicer._execution._registered_chain_wallets is not None
        assert set(servicer._execution._registered_chain_wallets.keys()) == {"arbitrum"}
        assert servicer._execution._registered_chain_wallets["arbitrum"].lower() == TEST_EOA_ADDRESS.lower()

    @pytest.mark.asyncio
    async def test_registered_chain_wallets_is_none_when_empty(self, monkeypatch) -> None:
        """If ``chain_wallet_map`` is empty (all chains invalid) AND no registry,
        _registered_chain_wallets is left as None."""
        monkeypatch.delenv("ALMANAK_GATEWAY_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        # Wallet registry present but empty, so we get past the early wallet-missing
        # error; then all chains are invalid -> empty map.
        registry = MagicMock()
        registry.resolve.side_effect = RuntimeError("no entry")
        registry.all_chains.return_value = []

        settings = GatewaySettings(
            private_key=None,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        settings.private_key = None
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["not_real_chain"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is False
        # Empty map -> None assignment
        assert servicer._execution._registered_chain_wallets is None

    @pytest.mark.asyncio
    async def test_registered_chains_set_assigned(self) -> None:
        servicer = _make_servicer(_settings_eoa())
        request = _FakeRegisterChainsRequest(chains=["arbitrum", "base"], wallet_address="")

        await servicer.RegisterChains(request, make_grpc_context())

        assert servicer._execution._registered_chains == {"arbitrum", "base"}


# ---------------------------------------------------------------------------
# Market reinit
# ---------------------------------------------------------------------------
class TestMarketReinit:
    @pytest.mark.asyncio
    async def test_market_reinit_called_with_first_initialized_chain(self) -> None:
        market = MagicMock()
        market.reinitialize = AsyncMock()
        servicer = _make_servicer(_settings_eoa(), market_servicer=market)
        request = _FakeRegisterChainsRequest(chains=["arbitrum", "base"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        market.reinitialize.assert_awaited_once_with("arbitrum")

    @pytest.mark.asyncio
    async def test_market_reinit_not_called_when_no_chains_initialized(self) -> None:
        market = MagicMock()
        market.reinitialize = AsyncMock()
        servicer = _make_servicer(_settings_eoa(), market_servicer=market)
        servicer._execution._get_orchestrator.side_effect = RuntimeError("kaboom")
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        await servicer.RegisterChains(request, make_grpc_context())

        market.reinitialize.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_market_reinit_failure_is_swallowed(self) -> None:
        market = MagicMock()
        market.reinitialize = AsyncMock(side_effect=RuntimeError("reinit boom"))
        servicer = _make_servicer(_settings_eoa(), market_servicer=market)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        # Exception swallowed - response still success
        assert response.success is True
        assert list(response.initialized_chains) == ["arbitrum"]

    @pytest.mark.asyncio
    async def test_no_market_servicer_is_fine(self) -> None:
        """Servicer constructed with market_servicer=None should not error."""
        servicer = _make_servicer(_settings_eoa(), market_servicer=None)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True


# ---------------------------------------------------------------------------
# Legacy wallet_address field in response
# ---------------------------------------------------------------------------
class TestLegacyWalletField:
    @pytest.mark.asyncio
    async def test_legacy_wallet_uses_request_wallet_when_provided(self) -> None:
        servicer = _make_servicer(_settings_safe())
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address=EXPLICIT_WALLET)

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.wallet_address == EXPLICIT_WALLET

    @pytest.mark.asyncio
    async def test_legacy_wallet_falls_back_to_first_initialized_registry_wallet(self, monkeypatch) -> None:
        """When no legacy wallet is derivable, use first initialized chain's registry wallet."""
        monkeypatch.delenv("ALMANAK_GATEWAY_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        registry = MagicMock()
        registry.resolve.side_effect = lambda c: _FakeResolvedWallet(account_address="0xregistryA")
        registry.all_chains.return_value = ["arbitrum"]

        settings = GatewaySettings(
            private_key=None,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        settings.private_key = None
        servicer = _make_servicer(settings, wallet_registry=registry)
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.wallet_address == "0xregistryA"


# ---------------------------------------------------------------------------
# Response shape / success path
# ---------------------------------------------------------------------------
class TestResponseShape:
    @pytest.mark.asyncio
    async def test_success_response_populates_expected_fields(self) -> None:
        servicer = _make_servicer(_settings_eoa())
        request = _FakeRegisterChainsRequest(chains=["arbitrum", "base"], wallet_address=EXPLICIT_WALLET)

        response = await servicer.RegisterChains(request, make_grpc_context())

        assert response.success is True
        assert response.error == ""
        assert list(response.initialized_chains) == ["arbitrum", "base"]
        assert response.wallet_address == EXPLICIT_WALLET
        # chain_wallets populated even without registry (built from chain_wallet_map)
        wallets = dict(response.chain_wallets)
        assert wallets == {"arbitrum": EXPLICIT_WALLET, "base": EXPLICIT_WALLET}

    @pytest.mark.asyncio
    async def test_context_set_code_not_called_on_success(self) -> None:
        """RegisterChains does NOT call context.set_code - error is carried in-proto."""
        servicer = _make_servicer(_settings_eoa())
        context = make_grpc_context()
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        await servicer.RegisterChains(request, context)

        context.set_code.assert_not_called()
        context.set_details.assert_not_called()

    @pytest.mark.asyncio
    async def test_context_set_code_not_called_on_error(self, monkeypatch) -> None:
        """Even on error response, RegisterChains does NOT call context.set_code."""
        monkeypatch.delenv("ALMANAK_GATEWAY_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_PRIVATE_KEY", raising=False)

        settings = GatewaySettings(
            private_key=None,
            safe_address=None,
            safe_mode=None,
            metrics_enabled=False,
            audit_enabled=False,
        )
        settings.private_key = None
        servicer = _make_servicer(settings)
        context = make_grpc_context()
        request = _FakeRegisterChainsRequest(chains=["arbitrum"], wallet_address="")

        await servicer.RegisterChains(request, context)

        # Pin: RegisterChains uses response.error instead of gRPC status.
        context.set_code.assert_not_called()
        context.set_details.assert_not_called()
