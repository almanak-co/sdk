"""Branch coverage for ExecutionServiceServicer._create_signer_from_resolved.

Drives every wallet.kind dispatch branch with lightweight resolved-wallet stubs:
zodiac (config eoa / settings eoa / key-derived eoa / no eoa), direct (wallet
key / settings key / no key), squads (not implemented), unknown kinds, and the
missing-kind default. Safe signer construction is patched out; LocalKeySigner is
constructed for real with well-known Anvil dev keys. No RPC access.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.execution.signer import LocalKeySigner
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.execution_service import ExecutionServiceServicer

# Anvil default dev keys #0 and #1 and their derived addresses (public knowledge).
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_DERIVED_EOA = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
WALLET_PRIVATE_KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
WALLET_DERIVED_EOA = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

EXPLICIT_EOA = "0x5201565562a45db04419f4c3d582d3ad38ad8bca"
TEST_SAFE_ADDRESS = "0x88c0fede55dfca0512c1a013c2ba118706cd4ae2"
TEST_ZODIAC_ADDRESS = "0xa7cfda03e0ccc7d5c119de9390269a1804f73b68"

_PATCH_TARGET = "almanak.framework.execution.signer.safe.create_safe_signer"


def _service(
    *,
    private_key: str | None = None,
    eoa_address: str | None = None,
    signer_service_url: str | None = None,
    signer_service_jwt: str | None = None,
) -> ExecutionServiceServicer:
    settings = GatewaySettings(
        private_key=private_key,
        eoa_address=eoa_address,
        signer_service_url=signer_service_url,
        signer_service_jwt=signer_service_jwt,
        metrics_enabled=False,
        audit_enabled=False,
    )
    # Force the exact values in case a local .env supplies fallbacks.
    settings.private_key = private_key
    settings.eoa_address = eoa_address
    settings.signer_service_url = signer_service_url
    settings.signer_service_jwt = signer_service_jwt
    return ExecutionServiceServicer(settings)


def _zodiac_wallet(config: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        kind="zodiac",
        account_address=TEST_SAFE_ADDRESS,
        chain="arbitrum",
        config={"zodiac_roles_address": TEST_ZODIAC_ADDRESS, **(config or {})},
    )


class TestZodiacBranch:
    def test_config_eoa_address_wins(self):
        service = _service(private_key=TEST_PRIVATE_KEY, eoa_address=TEST_DERIVED_EOA)
        wallet = _zodiac_wallet({"eoa_address": EXPLICIT_EOA})

        with patch(_PATCH_TARGET) as mock_create:
            mock_create.return_value = MagicMock()
            signer = service._create_signer_from_resolved(wallet)

        assert signer is mock_create.return_value
        safe_config = mock_create.call_args[0][0]
        assert safe_config.mode == "zodiac"
        assert safe_config.wallet_config.eoa_address.lower() == EXPLICIT_EOA.lower()
        assert safe_config.wallet_config.safe_address.lower() == TEST_SAFE_ADDRESS.lower()
        assert safe_config.wallet_config.zodiac_roles_address.lower() == TEST_ZODIAC_ADDRESS.lower()
        assert safe_config.private_key == TEST_PRIVATE_KEY

    def test_settings_eoa_address_preferred_over_key_derivation(self):
        service = _service(
            eoa_address=EXPLICIT_EOA,
            signer_service_url="https://signer.example.com",
            signer_service_jwt="jwt-token",
        )
        wallet = _zodiac_wallet()

        with patch(_PATCH_TARGET) as mock_create:
            mock_create.return_value = MagicMock()
            service._create_signer_from_resolved(wallet)

        safe_config = mock_create.call_args[0][0]
        assert safe_config.wallet_config.eoa_address.lower() == EXPLICIT_EOA.lower()
        # Remote-signer settings are threaded through to the Safe config.
        assert safe_config.signer_service_url == "https://signer.example.com"
        assert safe_config.signer_service_jwt == "jwt-token"

    def test_eoa_derived_from_settings_private_key(self):
        service = _service(private_key=TEST_PRIVATE_KEY)
        wallet = _zodiac_wallet()

        with patch(_PATCH_TARGET) as mock_create:
            mock_create.return_value = MagicMock()
            service._create_signer_from_resolved(wallet)

        safe_config = mock_create.call_args[0][0]
        assert safe_config.wallet_config.eoa_address.lower() == TEST_DERIVED_EOA.lower()

    def test_no_eoa_source_raises(self):
        service = _service()
        wallet = _zodiac_wallet()

        with pytest.raises(ValueError, match="requires eoa_address"):
            service._create_signer_from_resolved(wallet)


class TestDirectBranch:
    def test_wallet_private_key_preferred(self):
        service = _service(private_key=TEST_PRIVATE_KEY)
        wallet = SimpleNamespace(kind="direct", private_key=WALLET_PRIVATE_KEY)

        signer = service._create_signer_from_resolved(wallet)

        assert isinstance(signer, LocalKeySigner)
        assert signer.address == WALLET_DERIVED_EOA

    def test_falls_back_to_settings_private_key(self):
        service = _service(private_key=TEST_PRIVATE_KEY)
        wallet = SimpleNamespace(kind="direct", private_key=None)

        signer = service._create_signer_from_resolved(wallet)

        assert isinstance(signer, LocalKeySigner)
        assert signer.address == TEST_DERIVED_EOA

    def test_no_key_anywhere_raises(self):
        service = _service()
        wallet = SimpleNamespace(kind="direct", private_key=None)

        with pytest.raises(ValueError, match="Direct wallet requires private_key"):
            service._create_signer_from_resolved(wallet)

    def test_missing_kind_defaults_to_direct(self):
        service = _service(private_key=TEST_PRIVATE_KEY)
        # No `kind` attribute at all: getattr default routes to the direct
        # branch, and no `private_key` attribute means settings key is used.
        wallet = SimpleNamespace()

        signer = service._create_signer_from_resolved(wallet)

        assert isinstance(signer, LocalKeySigner)
        assert signer.address == TEST_DERIVED_EOA


class TestOtherKinds:
    def test_squads_not_implemented(self):
        service = _service(private_key=TEST_PRIVATE_KEY)
        wallet = SimpleNamespace(kind="squads")

        with pytest.raises(NotImplementedError, match="Squads multisig"):
            service._create_signer_from_resolved(wallet)

    def test_unknown_kind_rejected(self):
        service = _service(private_key=TEST_PRIVATE_KEY)
        wallet = SimpleNamespace(kind="frobnicate")

        with pytest.raises(ValueError, match="Unknown wallet kind: frobnicate"):
            service._create_signer_from_resolved(wallet)


# =============================================================================
# _get_orchestrator — chain/wallet orchestrator construction and caching
# =============================================================================

_ORCH_PATCH = "almanak.framework.execution.orchestrator.ExecutionOrchestrator"
_SIM_PATCH = "almanak.framework.execution.simulator.create_simulator"
_SUB_PATCH = "almanak.framework.execution.submitter.PublicMempoolSubmitter"
_RPC_PATCH = "almanak.gateway.utils.get_rpc_url"
_RPC_URL = "http://rpc.test.local:8545"


def _orchestrator_harness():
    """Patch the four collaborators _get_orchestrator imports lazily."""
    import contextlib

    stack = contextlib.ExitStack()
    orch_cls = stack.enter_context(patch(_ORCH_PATCH))
    orch_cls.return_value.tx_risk_config.max_gas_price_gwei = 321
    sim = stack.enter_context(patch(_SIM_PATCH))
    sub = stack.enter_context(patch(_SUB_PATCH))
    rpc = stack.enter_context(patch(_RPC_PATCH, return_value=_RPC_URL))
    return stack, orch_cls, sim, sub, rpc


def _eoa_service() -> ExecutionServiceServicer:
    service = _service(private_key=TEST_PRIVATE_KEY)
    # Deterministic non-Safe default-signer path regardless of local .env.
    service.settings.safe_address = None
    service.settings.safe_mode = None
    return service


class TestGetOrchestrator:
    @pytest.mark.asyncio
    async def test_cache_hit_returns_existing_orchestrator_untouched(self):
        service = _eoa_service()
        cached = MagicMock(name="cached-orchestrator")
        service._orchestrator_cache["arbitrum:0xabc"] = cached

        stack, orch_cls, _sim, _sub, rpc = _orchestrator_harness()
        with stack:
            result = await service._get_orchestrator("arbitrum", "0xabc")

        assert result is cached
        orch_cls.assert_not_called()
        rpc.assert_not_called()

    @pytest.mark.asyncio
    async def test_default_signer_path_builds_and_caches_orchestrator(self):
        service = _eoa_service()

        stack, orch_cls, sim, sub, rpc = _orchestrator_harness()
        with stack:
            result = await service._get_orchestrator("arbitrum", TEST_DERIVED_EOA)

        assert result is orch_cls.return_value
        rpc.assert_called_once_with("arbitrum", network=service.settings.network)
        sub.assert_called_once_with(rpc_url=_RPC_URL)
        sim.assert_called_once_with(rpc_url=_RPC_URL)

        kwargs = orch_cls.call_args.kwargs
        assert isinstance(kwargs["signer"], LocalKeySigner)
        assert kwargs["signer"].address == TEST_DERIVED_EOA
        assert kwargs["chain"] == "arbitrum"
        assert kwargs["rpc_url"] == _RPC_URL
        assert kwargs["submitter"] is sub.return_value
        assert kwargs["simulator"] is sim.return_value

        cache_key = f"arbitrum:{TEST_DERIVED_EOA}"
        assert service._orchestrator_cache[cache_key] is result
        assert cache_key in service._orchestrator_locks
        assert service._orchestrator_default_gas_caps[cache_key] == 321

    @pytest.mark.asyncio
    async def test_registry_resolved_wallet_supplies_signer(self):
        service = _eoa_service()
        registry = MagicMock()
        registry.resolve.return_value = SimpleNamespace(kind="direct", private_key=WALLET_PRIVATE_KEY)
        service.wallet_registry = registry

        stack, orch_cls, _sim, _sub, _rpc = _orchestrator_harness()
        with stack:
            await service._get_orchestrator("base", TEST_DERIVED_EOA)

        registry.resolve.assert_called_once_with("base")
        signer = orch_cls.call_args.kwargs["signer"]
        assert isinstance(signer, LocalKeySigner)
        # Registry wallet key wins over the settings key.
        assert signer.address == WALLET_DERIVED_EOA

    @pytest.mark.asyncio
    async def test_registry_returning_none_falls_back_to_default_signer(self):
        service = _eoa_service()
        registry = MagicMock()
        registry.resolve.return_value = None
        service.wallet_registry = registry

        stack, orch_cls, _sim, _sub, _rpc = _orchestrator_harness()
        with stack:
            await service._get_orchestrator("base", TEST_DERIVED_EOA)

        signer = orch_cls.call_args.kwargs["signer"]
        assert isinstance(signer, LocalKeySigner)
        assert signer.address == TEST_DERIVED_EOA

    @pytest.mark.asyncio
    async def test_chain_missing_from_registry_falls_back_to_default_signer(self):
        service = _eoa_service()
        registry = MagicMock()
        registry.resolve.side_effect = KeyError("unknown chain")
        service.wallet_registry = registry

        stack, orch_cls, _sim, _sub, _rpc = _orchestrator_harness()
        with stack:
            await service._get_orchestrator("base", TEST_DERIVED_EOA)

        signer = orch_cls.call_args.kwargs["signer"]
        assert isinstance(signer, LocalKeySigner)
        assert signer.address == TEST_DERIVED_EOA

    @pytest.mark.asyncio
    async def test_resolved_wallet_signer_failure_fails_closed(self):
        # Registry resolves a wallet but signer construction fails: the
        # gateway must refuse to fall back to the default signer (funds
        # could otherwise route through the wrong wallet).
        service = _service()  # no settings key either
        service.settings.safe_address = None
        service.settings.safe_mode = None
        registry = MagicMock()
        registry.resolve.return_value = SimpleNamespace(kind="direct", private_key=None)
        service.wallet_registry = registry

        stack, orch_cls, _sim, _sub, _rpc = _orchestrator_harness()
        with stack:
            with pytest.raises(ValueError, match="Refusing to fall back to default signer"):
                await service._get_orchestrator("base", TEST_DERIVED_EOA)
            orch_cls.assert_not_called()

        assert service._orchestrator_cache == {}
        assert service._orchestrator_locks == {}
