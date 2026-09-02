"""Address-bound Slipstream LP_CLOSE / LP_COLLECT_FEES cross-check (ALM-3462 follow-on).

The NFT token id decides which physical position a close or collect acts on.
When the intent ALSO names the pool by bare address, that address must be the
pool the NFT's own manager generation and factory reconstruct — the same
contract the Uniswap V3 address-bound close enforces. A symbolic ``pool`` key
is informational and is not cross-checked (unchanged behaviour).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent

CURRENT, LEGACY = slipstream_lp_deployments("base")
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


def _adapter() -> MagicMock:
    adapter = MagicMock()
    adapter.resolve_owned_cl_deployment.return_value = CURRENT
    adapter.get_cl_position.return_value = SimpleNamespace(token0=WETH, token1=USDC, tick_spacing=50)
    return adapter


def _confirmed(pool: str = NFT_POOL) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool)


def _resolve(expected_pool: str | None, *, permission_discovery: bool = False):
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
            return_value=_confirmed(),
        ),
        patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=None) as verify,
    ):
        result = aerodrome_compiler._resolve_slipstream_position(
            compiler=_compiler(),
            adapter=_adapter(),
            token_id=777,
            intent_id="close-1",
            permission_discovery=permission_discovery,
            reviewed_deployments=(CURRENT, LEGACY),
            expected_pool=expected_pool,
        )
    return result, verify


def test_matching_bare_pool_is_admitted_case_insensitively() -> None:
    result, verify = _resolve(NFT_POOL.upper().replace("0X", "0x"))
    assert isinstance(result, aerodrome_compiler._ResolvedSlipstreamPosition)
    assert result.deployment == CURRENT
    verify.assert_called_once()


def test_mismatched_bare_pool_is_refused_before_verifier() -> None:
    result, verify = _resolve(OTHER_POOL)
    assert isinstance(result, CompilationResult)
    assert result.status is CompilationStatus.FAILED
    assert result.intent_id == "close-1"
    assert "tokenId=777 belongs to pool " + NFT_POOL in (result.error or "")
    assert OTHER_POOL in (result.error or "")
    assert "current generation" in (result.error or "")
    verify.assert_not_called()


def test_malformed_bare_pool_is_refused() -> None:
    result, _ = _resolve("0x1234")
    assert isinstance(result, CompilationResult)
    assert result.error == "Invalid exact Slipstream pool address: 0x1234"


@pytest.mark.parametrize("expected_pool", [None, "", "WETH/USDC/50"])
def test_symbolic_or_absent_pool_is_not_cross_checked(expected_pool: str | None) -> None:
    result, verify = _resolve(expected_pool)
    assert isinstance(result, aerodrome_compiler._ResolvedSlipstreamPosition)
    verify.assert_called_once()


def test_permission_discovery_skips_reads_and_cross_check() -> None:
    result, verify = _resolve(OTHER_POOL, permission_discovery=True)
    assert isinstance(result, aerodrome_compiler._ResolvedSlipstreamPosition)
    assert result.verified_venue is None
    verify.assert_not_called()


# Through the compilers: the intent's ``pool`` field reaches the cross-check
def _full_compiler() -> MagicMock:
    compiler = MagicMock()
    compiler.chain = "base"
    compiler._gateway_client = None
    compiler._get_chain_rpc_url.return_value = "http://localhost:8545"
    compiler._config = SimpleNamespace(permission_discovery=False)
    compiler.wallet_address = "0x" + "33" * 20
    compiler._validate_pool.return_value = None  # None == pool is fine
    return compiler


def _patched_adapter():
    return patch("almanak.connectors.aerodrome.AerodromeAdapter", return_value=_adapter())


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
    tx = MagicMock(gas_estimate=100_000, tx_type="decrease")
    tx.to_dict.return_value = {"tx_type": "decrease"}
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
            success=True, transactions=[tx], error=None
        )
        result = aerodrome_compiler.compile_lp_close_aerodrome_slipstream(_full_compiler(), intent)
    assert result.status is CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["nft_manager"] == CURRENT.position_manager


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
