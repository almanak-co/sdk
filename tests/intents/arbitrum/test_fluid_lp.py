"""Intent tests for Fluid DEX LP on Arbitrum.

4-Layer verification: compilation, execution, receipt parsing, balance deltas.
NO MOCKING — all tests use Anvil fork.

Layer 5 (accounting-persistence correctness) — epic VIB-4591, ticket VIB-4602.
Fluid DEX ``deposit()`` reverts on every pool on the Arbitrum fork (complex
Liquidity-layer routing), so LP_OPEN never lands on-chain and there is no
successful LP_OPEN / LP_CLOSE round-trip to assert a typed event against on
this connector. The reachable Layer-5 contract here is therefore the
FAILURE-path invariant from the merged V3 pilot
(``test_uniswap_v3_lp.py::test_lp_close_invalid_position_writes_no_accounting``):
a rejected/reverted LP intent must write a ledger row but enqueue NO
accounting_outbox row and persist NO typed ``accounting_events`` row. This is
the books-side mirror of the "no on-chain effect → no typed event" rule
(Empty≠Zero≠None, docs/internal/blueprints/27).

To run:
    uv run pytest tests/intents/arbitrum/test_fluid_lp.py -v -s
"""

from decimal import Decimal

import pytest

from almanak.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.connectors.fluid.sdk import FluidSDK, FluidSDKError
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.execution.result_enricher import enrich_result
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import assert_no_accounting_on_failure

pytestmark = pytest.mark.no_zodiac(reason="fluid connector not in manifest matrix")

CHAIN_NAME = "arbitrum"


def _execution_context(wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="layer5-fluid-lp",
        chain=CHAIN_NAME,
        wallet_address=wallet,
        protocol="fluid",
    )


def _enrich_for_accounting(execution_result, intent, wallet: str, bundle_metadata: dict | None = None):
    return enrich_result(
        execution_result,
        intent,
        _execution_context(wallet),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def _find_unencumbered_pool(sdk: FluidSDK):
    try:
        addresses = sdk.get_all_dex_addresses()
    except FluidSDKError:
        return None
    for addr in addresses:
        try:
            data = sdk.get_dex_data(addr)
            if not data.is_smart_collateral and not data.is_smart_debt:
                return (data.dex_address, data.token0, data.token1)
        except FluidSDKError:
            continue
    return None


@pytest.mark.integration
class TestFluidLPCompilation:
    @pytest.mark.intent(IntentType.LP_OPEN)
    def test_lp_open_fails_phase1(self, funded_wallet, anvil_rpc_url):
        """LP_OPEN compilation correctly returns FAILED in phase 1.

        Fluid DEX deposit() reverts on all pools due to complex Liquidity-layer routing.
        LP support is a follow-up. This test documents the expected phase 1 behavior.
        """
        sdk = FluidSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        pool_info = _find_unencumbered_pool(sdk)
        if pool_info is None:
            pytest.skip("No unencumbered Fluid DEX pool found on this fork")
        dex_address = pool_info[0]
        compiler = IntentCompiler(chain=CHAIN_NAME, wallet_address=funded_wallet, rpc_url=anvil_rpc_url)
        intent = LPOpenIntent(
            pool=dex_address,
            amount0=Decimal("0.001"),
            amount1=Decimal("0.001"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="fluid",
        )
        result = compiler.compile(intent)
        assert result.status.value == "FAILED", "LP_OPEN should fail in phase 1 (deposit not supported)"
        assert "not supported" in (result.error or "").lower()

    @pytest.mark.intent(IntentType.LP_CLOSE)
    def test_lp_close_compiles(self, funded_wallet, anvil_rpc_url):
        sdk = FluidSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        pool_info = _find_unencumbered_pool(sdk)
        if pool_info is None:
            pytest.skip("No unencumbered Fluid DEX pool found on this fork")
        compiler = IntentCompiler(chain=CHAIN_NAME, wallet_address=funded_wallet, rpc_url=anvil_rpc_url)
        intent = LPCloseIntent(position_id="1", protocol="fluid", pool=pool_info[0])
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

    @pytest.mark.intent(IntentType.LP_OPEN)
    def test_rejects_invalid_pool(self, funded_wallet, anvil_rpc_url):
        from almanak.framework.intents.compiler import IntentCompilerConfig

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = LPOpenIntent(
            pool="INVALID",
            amount0=Decimal("0.001"),
            amount1=Decimal("0.001"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="fluid",
        )
        result = compiler.compile(intent)
        assert result.status.value == "FAILED"


class TestFluidReceiptParsing:
    @pytest.mark.intent(IntentType.LP_OPEN)
    def test_parser_extracts_nft_id(self):
        from tests.unit.connectors.fluid.test_fluid_receipt_parser import _log_operate, _make_receipt

        parser = FluidReceiptParser()
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=1_000_000, token1_amt=2_000_000)])
        assert parser.extract_position_id(receipt) == 42

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    def test_supported_extractions(self):
        parser = FluidReceiptParser()
        assert "position_id" in parser.SUPPORTED_EXTRACTIONS
        assert "lp_close_data" in parser.SUPPORTED_EXTRACTIONS


@pytest.mark.integration
class TestEncumbranceGuard:
    @pytest.mark.intent(IntentType.LP_OPEN)
    def test_sdk_pool_data_readable(self, anvil_rpc_url):
        """Verify pool data is readable from on-chain (smoke test)."""
        sdk = FluidSDK(chain=CHAIN_NAME, rpc_url=anvil_rpc_url)
        try:
            addresses = sdk.get_all_dex_addresses()
        except FluidSDKError:
            pytest.skip("Cannot enumerate Fluid DEX pools")
        if not addresses:
            pytest.skip("No Fluid DEX pools on this fork")
        data = sdk.get_dex_data(addresses[0])
        assert isinstance(data.is_smart_collateral, bool)
        assert isinstance(data.is_smart_debt, bool)


# =============================================================================
# Layer 5 — accounting-persistence correctness (epic VIB-4591, ticket VIB-4602)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.lp
class TestFluidLPAccounting:
    """Layer-5 failure-path accounting assertions for Fluid DEX LP.

    Fluid DEX ``deposit()`` reverts on every pool on the Arbitrum fork, so
    LP_OPEN cannot land on-chain (``test_lp_open_fails_phase1`` documents the
    phase-1 FAILED contract). With no successful round-trip there is no typed
    LP_OPEN / LP_CLOSE event to assert; the reachable Layer-5 contract is the
    FAILURE-path invariant: a rejected LP intent persists a ledger row but
    enqueues no accounting_outbox row and writes no typed ``accounting_events``
    row. Both reachable failure shapes (unsupported LP_OPEN, invalid LP_CLOSE)
    are asserted via the shared ``assert_no_accounting_on_failure`` helper used
    by the merged V3 pilot.

    These use an explicit invalid pool (``"INVALID"``) with
    ``allow_placeholder_prices`` — the same deterministic FAILED-compile path
    as ``test_rejects_invalid_pool`` — so the no-accounting-on-failure contract
    is actually exercised on CI rather than skipped behind on-fork pool
    discovery (``_find_unencumbered_pool`` returns None on the CI fork).
    """

    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_unsupported_writes_no_accounting(
        self,
        funded_wallet,
        anvil_rpc_url,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """A rejected LP_OPEN (FAILED compile) writes no typed accounting rows."""
        from almanak.framework.intents.compiler import IntentCompilerConfig

        intent = LPOpenIntent(
            pool="INVALID",
            amount0=Decimal("0.001"),
            amount1=Decimal("0.001"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="fluid",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "FAILED", (
            f"Fluid LP_OPEN on an invalid pool must compile FAILED: {compilation_result.error}"
        )
        failed_result = ExecutionResult(
            success=False,
            phase=ExecutionPhase.VALIDATION,
            error=compilation_result.error or "LP_OPEN compilation failed",
        )

        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=intent,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            eth_call_reader=anvil_eth_call_adapter,
        )

    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_invalid_position_writes_no_accounting(
        self,
        funded_wallet,
        orchestrator: ExecutionOrchestrator,
        anvil_rpc_url,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """A rejected/reverted LP_CLOSE on an invalid pool writes no typed rows."""
        from almanak.framework.intents.compiler import IntentCompilerConfig

        invalid_close = LPCloseIntent(
            position_id="999999999999",
            pool="INVALID",
            collect_fees=True,
            protocol="fluid",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            rpc_url=anvil_rpc_url,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        compilation_result = compiler.compile(invalid_close)

        if compilation_result.status.value == "SUCCESS" and compilation_result.action_bundle is not None:
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            assert not execution_result.success, "invalid LP_CLOSE setup must not land successfully"
            failed_result = _enrich_for_accounting(
                execution_result,
                invalid_close,
                funded_wallet,
                compilation_result.action_bundle.metadata,
            )
        else:
            assert compilation_result.status.value == "FAILED"
            failed_result = ExecutionResult(
                success=False,
                phase=ExecutionPhase.VALIDATION,
                error=compilation_result.error or "LP_CLOSE compilation failed",
            )

        await assert_no_accounting_on_failure(
            layer5_accounting_harness,
            intent=invalid_close,
            result=failed_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            eth_call_reader=anvil_eth_call_adapter,
        )
