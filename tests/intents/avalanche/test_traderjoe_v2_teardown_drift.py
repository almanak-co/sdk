"""TraderJoe V2 LP teardown active-bin-drift regression test (VIB-3742).

Pins the framework's hardening against the silent-leak bug: an LP_CLOSE
intent that omits ``protocol_params['bin_ids']`` falls back to the
compiler's active_id +/- 50 bin heuristic. After price drift the original
bins may sit outside that window and ``removeLiquidity`` then closes only
a subset; the framework otherwise reports success while liquidity remains
stranded on-chain. (Root cause of the $1.16 leak that prompted VIB-3741 /
VIB-3742.)

This test exercises both directions:

1. **Buggy path** (LP_CLOSE without ``bin_ids`` after active-id drift):
   the compiler emits a WARNING (item 1 of VIB-3742) and the heuristic
   misses bins outside the +/- 50 window. We assert non-zero residual LB
   token balance remains on-chain.
2. **Canonical fix** (LP_CLOSE with ``bin_ids`` populated): every original
   bin's balance goes to zero — full closure.

We drift the active bin via ``anvil_setStorageAt`` against the LBPair's
parameter slot rather than a multi-million-dollar synthetic swap. The
storage write is a test-only mechanism — production code never writes
LBPair storage directly. This is allowed under the gateway boundary
because it runs against the local Anvil fork only; no external network
egress occurs.

All four intent-test verification layers are exercised:

- Layer 1 (compilation): both compile to valid ActionBundles.
- Layer 2 (execution): both execute successfully on Anvil.
- Layer 3 (receipt parsing): WithdrawnFromBins events present in receipts.
- Layer 4 (balance deltas): bilateral on-chain LB token balance check
  with exact "all bins -> 0" assertions for the strong-mode close, and
  "some bins still > 0" for the heuristic.

To run:
    uv run pytest tests/intents/avalanche/test_traderjoe_v2_teardown_drift.py -v -s
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.traderjoe_v2 import (
    TraderJoeV2Adapter,
    TraderJoeV2Config,
)
from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, LPCloseIntent, LPOpenIntent
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    get_token_balance,
)
from tests.intents.pool_helpers import fail_if_traderjoe_pool_missing

logger = logging.getLogger(__name__)


CHAIN_NAME = "avalanche"
POOL = "WAVAX/USDC/20"
BIN_STEP = 20

# A wider position than the canonical LP test — we want enough bins
# spanning the active id that some land outside the +/- 50 window after
# we drift active_id.
LP_AMOUNT_WAVAX = Decimal("2.0")
LP_AMOUNT_USDC = Decimal("50")
RANGE_LOWER = Decimal("5")
RANGE_UPPER = Decimal("500")

# How far to push the active bin away from its starting value. The
# compiler heuristic scans active_id +/- 50 bins, so we must move at
# least 51 to guarantee the original bins fall outside the window.
ACTIVE_BIN_DRIFT_DELTA = 75


# =============================================================================
# Helpers
# =============================================================================


def _adapter(rpc_url: str, wallet: str) -> TraderJoeV2Adapter:
    """Build a TJ V2 adapter for raw SDK reads (post-condition style)."""
    return TraderJoeV2Adapter(
        TraderJoeV2Config(
            chain=CHAIN_NAME,
            wallet_address=wallet,
            rpc_url=rpc_url,
        )
    )


def _get_active_id(rpc_url: str, pool_address: str) -> int:
    """Read the LBPair active_id directly via web3."""
    adapter = _adapter(rpc_url, "0x0000000000000000000000000000000000000001")
    pair = adapter.sdk.get_pair_contract(pool_address)
    return int(pair.functions.getActiveId().call())


def _bins_with_balance(
    rpc_url: str, pool_address: str, wallet: str, bin_ids: list[int]
) -> dict[int, int]:
    """Return non-zero LB token balances for ``bin_ids`` (strong-mode read)."""
    adapter = _adapter(rpc_url, wallet)
    return adapter.sdk.get_position_balances_for_ids(pool_address, wallet, bin_ids)


def _drift_active_bin(rpc_url: str, pool_address: str, delta: int) -> tuple[int, int]:
    """Push the LBPair's ``active_id`` by ``delta`` via ``anvil_setStorageAt``.

    The LBPair contract packs its parameters into a single ``bytes32``
    field. Per joe-v2 ``PairParameterHelper.sol``, ``active_id`` is stored
    at bit offset **224** (24 bits wide) inside that packed value. The
    storage slot of the field varies by LBPair version; we locate it at
    runtime by probing candidate slots and matching the decoded value
    against the canonical ``getActiveId()`` accessor — that way the test
    is robust against future layout changes.

    The write is verified by re-reading ``getActiveId()`` after the
    storage poke; on mismatch we ``pytest.skip`` cleanly rather than
    proceed with a misleading "drift didn't happen" assertion.

    Returns ``(original_active_id, drifted_active_id)``.
    """
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    # Canonical reader — also our source of truth for "did the write take?".
    adapter = _adapter(rpc_url, "0x0000000000000000000000000000000000000001")
    pair = adapter.sdk.get_pair_contract(pool_address)
    original_active_id = int(pair.functions.getActiveId().call())
    new_active_id = original_active_id + delta

    # active_id occupies bits [224, 248) of the packed _parameters bytes32.
    OFFSET_ACTIVE_ID = 224
    ACTIVE_ID_WIDTH = 24
    ACTIVE_ID_MASK = (1 << ACTIVE_ID_WIDTH) - 1
    BITMASK = ACTIVE_ID_MASK << OFFSET_ACTIVE_ID

    # Probe candidate slots: locate the one whose decoded active_id at
    # offset 224 matches getActiveId(). joe-v2 v2.2 uses slot 16, but
    # earlier versions / forks may differ — search a small range.
    found_slot: int | None = None
    found_value: int = 0
    for candidate in (16, 5, 6, 7, 8, 4, 3, 9, 10, 11, 12, 13, 14, 15):
        raw = w3.eth.get_storage_at(pool_address, candidate)
        raw_int = int.from_bytes(raw, "big") if isinstance(raw, bytes) else int(raw, 16)
        if ((raw_int & BITMASK) >> OFFSET_ACTIVE_ID) == original_active_id:
            found_slot = candidate
            found_value = raw_int
            break
    if found_slot is None:
        pytest.skip(
            f"Could not locate LBPair _parameters slot for active_id="
            f"{original_active_id}; contract layout may differ from joe-v2 v2.2."
        )

    new_params = (found_value & ~BITMASK) | ((new_active_id & ACTIVE_ID_MASK) << OFFSET_ACTIVE_ID)
    new_hex = "0x" + format(new_params, "x").rjust(64, "0")
    slot_hex = "0x" + format(found_slot, "x").rjust(64, "0")
    w3.provider.make_request("anvil_setStorageAt", [pool_address, slot_hex, new_hex])

    # Verify the write propagated through the canonical accessor — proves
    # we hit the right slot/offset, not just "wrote what we wrote back".
    landed = int(pair.functions.getActiveId().call())
    if landed != new_active_id:
        pytest.skip(
            f"Storage write did not affect getActiveId() (wrote "
            f"{new_active_id}, reads {landed}); active_id may be derived "
            f"from a slot we did not probe."
        )

    return original_active_id, new_active_id


async def _open_position(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> tuple[str, list[int]]:
    """Open a TJ V2 LP position. Returns (pool_address, captured_bin_ids)."""
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_WAVAX,
        amount1=LP_AMOUNT_USDC,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="traderjoe_v2",
        chain=CHAIN_NAME,
    )

    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compiled = compiler.compile(intent)
    assert compiled.status.value == "SUCCESS", f"LP_OPEN compilation: {compiled.error}"
    assert compiled.action_bundle is not None

    exec_result = await orchestrator.execute(compiled.action_bundle)
    assert exec_result.success, f"LP_OPEN execution: {exec_result.error}"

    parser = TraderJoeV2ReceiptParser()
    captured: list[int] = []
    for tx_result in exec_result.transaction_results:
        if tx_result.receipt:
            bin_ids = parser.extract_bin_ids(tx_result.receipt.to_dict())
            if bin_ids:
                captured = list(bin_ids)
                break
    assert captured, "LP_OPEN must surface bin_ids from DepositedToBins event"

    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    pool_address = _adapter(anvil_rpc_url, funded_wallet).sdk.get_pool_address(
        tokens["WAVAX"], tokens["USDC"], BIN_STEP
    )
    return pool_address, captured


# =============================================================================
# Test
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.lp
class TestTraderJoeV2TeardownDrift:
    """Pins VIB-3742 framework hardening for TJ V2 LP teardown."""

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_without_bin_ids_after_active_drift_leaks(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """LP_CLOSE without bin_ids after active-id drift LEAKS liquidity.

        This pins the buggy path: when the original bins fall outside the
        compiler's heuristic window, the close skips them and the
        post-close on-chain balance for the original bins is NON-ZERO.

        Layers exercised:
        - Layer 1: compilation succeeds for both LP_OPEN and LP_CLOSE.
        - Layer 2: execution succeeds.
        - Layer 3: receipt parsing returns WithdrawnFromBins (partial).
        - Layer 4: residual LB token balance > 0 across original bins.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, BIN_STEP)

        pool_address, captured_bin_ids = await _open_position(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )

        # Confirm we actually have LB tokens across the captured bins.
        balances_before = _bins_with_balance(
            anvil_rpc_url, pool_address, funded_wallet, captured_bin_ids
        )
        total_before = sum(balances_before.values())
        assert total_before > 0, "Position must hold non-zero LB tokens before close"

        # Capture wallet token balances pre-close for Layer 4 bilateral
        # delta assertions on the heuristic path. The heuristic close *does*
        # withdraw the bins inside the +/- 50 window, so wallet balances
        # MUST increase — assert that to pin Layer 4 the same way the
        # strong-mode path does. ``wavax_addr`` / ``usdc_addr`` are already
        # bound earlier in this test from CHAIN_CONFIGS.
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        # Drift active_id past the +/- 50 bin heuristic window.
        original_active, drifted_active = _drift_active_bin(
            anvil_rpc_url, pool_address, ACTIVE_BIN_DRIFT_DELTA
        )
        assert abs(drifted_active - original_active) > 50, (
            f"Drift insufficient to escape the +/- 50 window "
            f"(orig={original_active} drifted={drifted_active})"
        )

        # Confirm: every original bin is now > 50 away from the drifted active.
        # Use min(...) — `max(...)>50` would only prove "at least one bin
        # outside", which is a weaker invariant than the leak guard requires.
        min_distance = min(abs(b - drifted_active) for b in captured_bin_ids)
        assert min_distance > 50, (
            f"Every captured bin must lie OUTSIDE the +/- 50 heuristic window "
            f"after drift. min_distance_to_active={min_distance} "
            f"(captured_bin_ids={captured_bin_ids}, drifted_active={drifted_active})"
        )

        # === Buggy close: omit bin_ids, hit the heuristic ===
        close_intent = LPCloseIntent(
            position_id="0",
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        with caplog.at_level(logging.WARNING):
            compiled_close = compiler.compile(close_intent)

        assert compiled_close.status.value == "SUCCESS", (
            f"Heuristic LP_CLOSE compilation: {compiled_close.error}"
        )
        assert compiled_close.action_bundle is not None

        # Item 1 of VIB-3742: compiler must emit a WARNING when bin_ids are
        # absent and the fallback fires.
        warned = any(
            "bin_ids" in record.getMessage() and "stranded" in record.getMessage()
            for record in caplog.records
            if record.levelno >= logging.WARNING
        )
        assert warned, (
            "Expected a WARNING-level log naming bin_ids and the silent "
            "partial-close risk when LP_CLOSE falls back to the heuristic. "
            f"Got: {[r.getMessage() for r in caplog.records]}"
        )

        exec_close = await orchestrator.execute(compiled_close.action_bundle)
        # The buggy close still succeeds at the TX level — the leak is silent.
        assert exec_close.success, f"Heuristic LP_CLOSE execution: {exec_close.error}"

        # Layer 3: assert at least one WithdrawnFromBins event was decoded.
        # The heuristic *did* remove SOME liquidity (whichever bins fell
        # inside the ±50 window post-drift), so a parser-decoded event must
        # be present. A bare smoke-parse would still pass if the parser
        # silently stopped recognising the event — pin the assertion.
        parser = TraderJoeV2ReceiptParser()
        heuristic_withdrawal_events = 0
        for tx_result in exec_close.transaction_results:
            if tx_result.receipt:
                parsed = parser.parse_receipt(tx_result.receipt.to_dict())
                if parsed.success:
                    heuristic_withdrawal_events += sum(
                        1
                        for event in parsed.events
                        if event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS
                    )
        assert heuristic_withdrawal_events > 0, (
            "Heuristic LP_CLOSE must still decode at least one "
            "WithdrawnFromBins event (it removes the bins inside the "
            "±50 window before leaking the rest)."
        )

        # Layer 4a: residual liquidity on the ORIGINAL bins must be > 0.
        # This is the bug: the heuristic missed those bins.
        balances_after = _bins_with_balance(
            anvil_rpc_url, pool_address, funded_wallet, captured_bin_ids
        )
        total_after = sum(balances_after.values())
        assert total_after > 0, (
            "Heuristic LP_CLOSE without bin_ids must LEAK liquidity in the "
            "drift scenario (regression guard for VIB-3741 / VIB-3742). "
            f"total_before={total_before} total_after={total_after}"
        )

        # Layer 4b: bilateral wallet token deltas — heuristic still withdrew
        # the bins inside the ±50 window, so both legs MUST move. This pins
        # the partial-close behaviour (something came back, just not all of
        # it) and would catch a regression that left wallet balances flat
        # while LB-bin balances also stayed flat.
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert wavax_after - wavax_before > 0, (
            f"Heuristic close must still increase WAVAX balance from the "
            f"bins inside the ±50 window. before={wavax_before} after={wavax_after}"
        )
        assert usdc_after - usdc_before > 0, (
            f"Heuristic close must still increase USDC balance from the "
            f"bins inside the ±50 window. before={usdc_before} after={usdc_after}"
        )

        logger.info(
            "Heuristic close leaked %d / %d LB tokens across %d original bins "
            "(wavax_delta=%d usdc_delta=%d)",
            total_after,
            total_before,
            len(captured_bin_ids),
            wavax_after - wavax_before,
            usdc_after - usdc_before,
        )

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_with_bin_ids_after_active_drift_closes_fully(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """LP_CLOSE WITH bin_ids closes fully even after active-id drift.

        This pins the canonical fix: when the strategy supplies the
        original bin_ids in protocol_params, the compiler uses the
        targeted balance lookup — drift is irrelevant.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, BIN_STEP)

        pool_address, captured_bin_ids = await _open_position(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )

        balances_before = _bins_with_balance(
            anvil_rpc_url, pool_address, funded_wallet, captured_bin_ids
        )
        assert sum(balances_before.values()) > 0

        _drift_active_bin(anvil_rpc_url, pool_address, ACTIVE_BIN_DRIFT_DELTA)

        # Capture pre-close token balances so Layer 4 can assert STRICT
        # positive deltas (just checking ``after > 0`` is a weak guard —
        # the funded wallet starts with non-zero balances, so absolute
        # post-checks pass even on a no-op close).
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        # === Strong-mode close: pass bin_ids ===
        close_intent = LPCloseIntent(
            position_id="0",
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
            protocol_params={"bin_ids": list(captured_bin_ids)},
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compiled = compiler.compile(close_intent)
        assert compiled.status.value == "SUCCESS", (
            f"Strong-mode LP_CLOSE compilation: {compiled.error}"
        )
        assert compiled.action_bundle is not None

        exec_close = await orchestrator.execute(compiled.action_bundle)
        assert exec_close.success, f"Strong-mode LP_CLOSE execution: {exec_close.error}"

        # Layer 3: WithdrawnFromBins event present.
        parser = TraderJoeV2ReceiptParser()
        found_withdrawal = False
        for tx_result in exec_close.transaction_results:
            if tx_result.receipt:
                parsed = parser.parse_receipt(tx_result.receipt.to_dict())
                if parsed.success:
                    for event in parsed.events:
                        if event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS:
                            found_withdrawal = True
                            break
        assert found_withdrawal, (
            "Strong-mode close must emit a WithdrawnFromBins event"
        )

        # Layer 4a: every original bin must be empty post-close.
        balances_after = _bins_with_balance(
            anvil_rpc_url, pool_address, funded_wallet, captured_bin_ids
        )
        assert balances_after == {}, (
            "Strong-mode close (bin_ids supplied) must zero EVERY original "
            f"bin's LB token balance. Residual: {balances_after}"
        )

        # Layer 4b: bilateral wallet token deltas — both sides must have
        # received funds back from the LBPair.
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        wavax_delta = wavax_after - wavax_before
        usdc_delta = usdc_after - usdc_before
        assert wavax_delta > 0, (
            f"Strong-mode close must increase WAVAX balance. "
            f"before={wavax_before} after={wavax_after}"
        )
        assert usdc_delta > 0, (
            f"Strong-mode close must increase USDC balance. "
            f"before={usdc_before} after={usdc_after}"
        )

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_framework_auto_inject_bin_ids_on_close_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """Item 2 of VIB-3742: framework auto-injection works end-to-end.

        Exercise the LPPositionTracker outside of the runner: simulate
        ``record_intent_execution`` after a real LP_OPEN, then call
        ``maybe_inject`` against an LP_CLOSE that was built without
        bin_ids. The returned intent must carry the captured bin_ids in
        ``protocol_params`` so the strong-mode compile path runs even
        though the strategy author did not supply them manually.
        """
        from almanak.framework.strategies.lp_position_tracker import LPPositionTracker

        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        wavax_addr = tokens["WAVAX"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, wavax_addr, usdc_addr, BIN_STEP)

        pool_address, captured_bin_ids = await _open_position(
            funded_wallet, orchestrator, price_oracle, anvil_rpc_url
        )

        # Simulate the open intent + result that the runner would record.
        open_intent = LPOpenIntent(
            pool=POOL,
            amount0=LP_AMOUNT_WAVAX,
            amount1=LP_AMOUNT_USDC,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )

        from types import SimpleNamespace

        execution_result = SimpleNamespace(
            bin_ids=list(captured_bin_ids),
            extracted_data={"bin_ids": list(captured_bin_ids)},
        )

        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            open_intent,
            success=True,
            result=execution_result,
            default_chain=CHAIN_NAME,
        )

        bare_close = LPCloseIntent(
            position_id="0",
            pool=POOL,
            collect_fees=True,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )
        injected = tracker.maybe_inject(bare_close, default_chain=CHAIN_NAME)

        assert injected is not bare_close, "Tracker must return a NEW intent on injection"
        assert injected.protocol_params is not None
        assert injected.protocol_params.get("bin_ids") == list(captured_bin_ids), (
            "Tracker must auto-inject the captured bin_ids unchanged"
        )

        # And the injected intent compiles + executes successfully —
        # i.e. the runner-equivalent path closes the position fully even
        # when the strategy author forgot the bin_ids.
        _drift_active_bin(anvil_rpc_url, pool_address, ACTIVE_BIN_DRIFT_DELTA)

        # Layer 4 capture: pre-close balances so we can assert deltas.
        wavax_before = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compiled = compiler.compile(injected)
        assert compiled.status.value == "SUCCESS"
        assert compiled.action_bundle is not None
        exec_close = await orchestrator.execute(compiled.action_bundle)
        assert exec_close.success

        # Layer 3: receipt-decoded WithdrawnFromBins event must be present
        # on the auto-injected close path too — pins parser behaviour.
        parser = TraderJoeV2ReceiptParser()
        found_withdrawal = False
        for tx_result in exec_close.transaction_results:
            if tx_result.receipt:
                parsed = parser.parse_receipt(tx_result.receipt.to_dict())
                if parsed.success and any(
                    event.event_type == TraderJoeV2EventType.WITHDRAWN_FROM_BINS
                    for event in parsed.events
                ):
                    found_withdrawal = True
                    break
        assert found_withdrawal, (
            "Auto-injected close must emit a decodable WithdrawnFromBins event"
        )

        # Layer 4a: every captured bin is now empty.
        balances_after = _bins_with_balance(
            anvil_rpc_url, pool_address, funded_wallet, captured_bin_ids
        )
        assert balances_after == {}, (
            f"Auto-injected close must achieve full closure. Residual: {balances_after}"
        )

        # Layer 4b: bilateral wallet deltas — auto-inject must move funds.
        wavax_after = get_token_balance(web3, wavax_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        assert wavax_after - wavax_before > 0, (
            f"Auto-injected close must increase WAVAX balance. "
            f"before={wavax_before} after={wavax_after}"
        )
        assert usdc_after - usdc_before > 0, (
            f"Auto-injected close must increase USDC balance. "
            f"before={usdc_before} after={usdc_after}"
        )
