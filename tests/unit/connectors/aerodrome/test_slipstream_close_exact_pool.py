"""Address-bound Slipstream LP_CLOSE / LP_COLLECT_FEES cross-check (ALM-3462 follow-on).

The NFT token id decides which physical position a close or collect acts on.
When the intent ALSO names the pool by bare address, that address must be the
pool the NFT's own manager generation and factory reconstruct — the same
contract the Uniswap V3 address-bound close enforces. A symbolic ``pool`` key
is informational and is not cross-checked (unchanged behaviour).

Permission discovery cannot read which generation owns a synthetic token id, so
it reads nothing and emits every reviewed generation's calldata instead.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.connectors.aerodrome.addresses import (
    SlipstreamDeployment,
    slipstream_deployment_for_factory,
    slipstream_lp_deployments,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent


def _generation(factory: str) -> SlipstreamDeployment:
    deployment = slipstream_deployment_for_factory("base", factory)
    assert deployment is not None, factory
    return deployment


CURRENT = _generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
LEGACY = _generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")
REVIEWED = slipstream_lp_deployments("base")
assert CURRENT in REVIEWED
assert LEGACY in REVIEWED
NFT_POOL = "0x3fe04a59ebd38cf06080a6f60a98d124eb59392a"
OTHER_POOL = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"
WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _compiler() -> SimpleNamespace:
    return SimpleNamespace(
        chain="base",
        _gateway_client=None,
        _get_chain_rpc_url=lambda: "http://localhost:8545",
        _validate_pool=lambda result, intent_id: None,
        _ctx=SimpleNamespace(venue_verification_gateway_factory=None),
    )


def _adapter(owner: SlipstreamDeployment = CURRENT) -> MagicMock:
    adapter = MagicMock()
    adapter.resolve_owned_cl_deployment.return_value = owner
    adapter.get_cl_position.return_value = SimpleNamespace(token0=WETH, token1=USDC, tick_spacing=50)
    return adapter


def _confirmed(pool: str = NFT_POOL, factory: str = CURRENT.factory) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool, factory=factory)


def _resolve(expected_pool: str | None, *, owner: SlipstreamDeployment = CURRENT):
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(factory=owner.factory),
        ) as validate,
        patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=None) as verify,
    ):
        result = aerodrome_compiler._resolve_slipstream_position(
            compiler=_compiler(),
            adapter=_adapter(owner),
            token_id=777,
            intent_id="close-1",
            expected_pool=expected_pool,
        )
    return result, verify, validate


def test_matching_bare_pool_is_admitted_case_insensitively() -> None:
    result, verify, _ = _resolve(NFT_POOL.upper().replace("0X", "0x"))
    assert isinstance(result, aerodrome_compiler._ResolvedSlipstreamPosition)
    assert result.deployment == CURRENT
    verify.assert_called_once()


@pytest.mark.parametrize("owner", [CURRENT, LEGACY], ids=["current", "legacy"])
def test_position_pool_is_reconstructed_on_the_owning_generation_factory(owner: SlipstreamDeployment) -> None:
    """The NFT's manager generation names the factory; no other generation is consulted."""
    result, verify, validate = _resolve(NFT_POOL, owner=owner)
    assert isinstance(result, aerodrome_compiler._ResolvedSlipstreamPosition)
    assert result.deployment == owner
    validate.assert_called_once()
    assert validate.call_args.args[:4] == ("base", WETH, USDC, 50)
    assert validate.call_args.kwargs["deployment"] == owner
    assert verify.call_args.kwargs["expected_position_manager"] == owner.position_manager


def test_mismatched_bare_pool_is_refused_before_verifier() -> None:
    result, verify, _ = _resolve(OTHER_POOL)
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "close-1"
    assert "tokenId=777 belongs to pool " + NFT_POOL in (result.error or "")
    assert OTHER_POOL in (result.error or "")
    assert "current generation" in (result.error or "")
    verify.assert_not_called()


def test_malformed_bare_pool_is_refused() -> None:
    result, _, _ = _resolve("0x1234")
    assert isinstance(result, CompilationResult)
    assert result.error == "Invalid exact Slipstream pool address: 0x1234"


@pytest.mark.parametrize("expected_pool", [None, "", "WETH/USDC/50"])
def test_symbolic_or_absent_pool_is_not_cross_checked(expected_pool: str | None) -> None:
    result, verify, _ = _resolve(expected_pool)
    assert isinstance(result, aerodrome_compiler._ResolvedSlipstreamPosition)
    verify.assert_called_once()


# Through the compilers: the intent's ``pool`` field reaches the cross-check
def _full_compiler(*, permission_discovery: bool = False) -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "base"
    compiler._gateway_client = None
    compiler._get_chain_rpc_url.return_value = "http://localhost:8545"
    compiler._config = SimpleNamespace(permission_discovery=permission_discovery)
    compiler.wallet_address = "0x" + "33" * 20
    compiler._validate_pool.return_value = None  # None == pool is fine
    return compiler


def _patched_adapter(adapter: MagicMock | None = None):
    return patch("almanak.connectors.aerodrome.AerodromeAdapter", return_value=adapter or _adapter())


def _tx(kind: str) -> MagicMock:
    tx = MagicMock(gas_estimate=100_000, tx_type=kind)
    tx.to_dict.return_value = {"tx_type": kind}
    return tx


def test_lp_close_with_wrong_bare_pool_builds_no_transactions() -> None:
    intent = LPCloseIntent(position_id="777", pool=OTHER_POOL, protocol="aerodrome_slipstream", chain="base")
    with (
        _patched_adapter() as adapter_cls,
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ),
        patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=None),
    ):
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_full_compiler(), intent)
    assert result.status is CompilationStatus.FAILED
    assert "not exact pool " + OTHER_POOL in (result.error or "")
    adapter_cls.return_value.remove_cl_liquidity.assert_not_called()


def test_lp_close_with_matching_bare_pool_proceeds() -> None:
    intent = LPCloseIntent(position_id="777", pool=NFT_POOL, protocol="aerodrome_slipstream", chain="base")
    with (
        _patched_adapter() as adapter_cls,
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ),
        patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=None),
    ):
        adapter_cls.return_value.remove_cl_liquidity.return_value = MagicMock(
            success=True, transactions=[_tx("decrease")], error=None
        )
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_full_compiler(), intent)
    assert result.status is CompilationStatus.SUCCESS, result.error
    assert adapter_cls.return_value.remove_cl_liquidity.call_args.kwargs["deployment"] == CURRENT
    assert result.action_bundle.metadata["nft_manager"] == CURRENT.position_manager
    assert result.action_bundle.metadata["slipstream_deployment"] == "current"


def test_collect_fees_with_wrong_bare_pool_builds_no_transactions() -> None:
    intent = CollectFeesIntent(
        pool=OTHER_POOL,
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": "777"},
    )
    with (
        _patched_adapter() as adapter_cls,
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ),
        patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=None),
    ):
        result = aerodrome_compiler.compile_collect_fees_aerodrome_slipstream(_full_compiler(), intent)
    assert result.status is CompilationStatus.FAILED
    assert "not exact pool " + OTHER_POOL in (result.error or "")
    adapter_cls.return_value.collect_cl_fees.assert_not_called()


# Permission discovery: no reads, no cross-check, every reviewed generation's calldata
def _discovery_adapter(method: str) -> MagicMock:
    adapter = _adapter()

    def _build(*, token_id: int, recipient: str, deployment: SlipstreamDeployment) -> MagicMock:
        return MagicMock(success=True, transactions=[_tx(f"{method}:{deployment.generation}")], error=None)

    getattr(adapter, method).side_effect = _build
    return adapter


def test_permission_discovery_skips_reads_and_cross_check() -> None:
    intent = LPCloseIntent(position_id="0", pool=OTHER_POOL, protocol="aerodrome_slipstream", chain="base")
    adapter = _discovery_adapter("remove_cl_liquidity")
    with (
        _patched_adapter(adapter),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool") as validate,
        patch.object(aerodrome_compiler, "_verify_slipstream_binding") as verify,
    ):
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(
            _full_compiler(permission_discovery=True), intent
        )

    assert result.status is CompilationStatus.SUCCESS, result.error
    adapter.resolve_owned_cl_deployment.assert_not_called()
    adapter.get_cl_position.assert_not_called()
    validate.assert_not_called()
    verify.assert_not_called()
    builds = [call.kwargs for call in adapter.remove_cl_liquidity.call_args_list]
    assert [build["deployment"] for build in builds] == list(REVIEWED)
    assert {build["token_id"] for build in builds} == {1}
    meta = result.action_bundle.metadata
    assert meta["nft_managers"] == [deployment.position_manager for deployment in REVIEWED]
    assert meta["slipstream_deployment"] == "all-reviewed"
    assert "nft_manager" not in meta
    assert [tx["tx_type"] for tx in result.action_bundle.transactions] == [
        f"remove_cl_liquidity:{deployment.generation}" for deployment in REVIEWED
    ]
    assert result.total_gas_estimate == 100_000 * len(REVIEWED)


def test_permission_discovery_collect_fees_emits_every_reviewed_generation() -> None:
    intent = CollectFeesIntent(
        pool=OTHER_POOL,
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": "0"},
    )
    adapter = _discovery_adapter("collect_cl_fees")
    with (
        _patched_adapter(adapter),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool") as validate,
        patch.object(aerodrome_compiler, "_verify_slipstream_binding") as verify,
    ):
        result = aerodrome_compiler.compile_collect_fees_aerodrome_slipstream(
            _full_compiler(permission_discovery=True), intent
        )

    assert result.status is CompilationStatus.SUCCESS, result.error
    adapter.resolve_owned_cl_deployment.assert_not_called()
    adapter.get_cl_position.assert_not_called()
    validate.assert_not_called()
    verify.assert_not_called()
    assert [call.kwargs["deployment"] for call in adapter.collect_cl_fees.call_args_list] == list(REVIEWED)
    meta = result.action_bundle.metadata
    assert meta["nft_managers"] == [deployment.position_manager for deployment in REVIEWED]
    assert meta["slipstream_deployment"] == "all-reviewed"
    assert [tx["tx_type"] for tx in result.action_bundle.transactions] == [
        f"collect_cl_fees:{deployment.generation}" for deployment in REVIEWED
    ]


def test_permission_discovery_fails_closed_when_one_generation_cannot_build() -> None:
    """A manifest missing one generation would revert that generation's close; refuse instead."""
    intent = LPCloseIntent(position_id="0", pool=OTHER_POOL, protocol="aerodrome_slipstream", chain="base")
    adapter = _adapter()

    def _build(*, token_id: int, recipient: str, deployment: SlipstreamDeployment) -> MagicMock:
        if deployment == LEGACY:
            return MagicMock(success=False, transactions=[], error="manager unavailable")
        return MagicMock(success=True, transactions=[_tx("decrease")], error=None)

    adapter.remove_cl_liquidity.side_effect = _build
    with _patched_adapter(adapter), patch("almanak.connectors.aerodrome.AerodromeConfig"):
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(
            _full_compiler(permission_discovery=True), intent
        )

    assert result.status is CompilationStatus.FAILED
    assert "legacy Slipstream generation" in (result.error or "")
    assert "manager unavailable" in (result.error or "")
    assert result.action_bundle is None
