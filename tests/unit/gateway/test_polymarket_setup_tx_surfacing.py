"""Setup-tx surfacing tests for PolymarketServiceServicer (VIB-3710).

The gateway returns a request-scoped ``setup_txs`` list from
``_ensure_wallet_ready`` — one entry per approval / wrap submitted on this
call. ``CreateAndPostOrder`` projects that list straight into the response
proto. There is NO instance-level ledger: each invocation owns its own list,
so concurrent order RPCs cannot leak attribution into each other's responses
(CodeRabbit thread 3 fix).

Coverage:

  g. ``_ensure_wallet_ready`` runs N setup txs -> returned list has N entries.
  h. ``_ensure_wallet_ready`` short-circuits (allowances applied, no wrap)
     -> returned list is empty.
  i. Two consecutive BUYs each return their own list (the second one's
     allowances-applied short-circuit yields []).
  i.5. The REAL ``_sign_and_submit_setup_tx`` driven concurrently with two
       distinct request-scoped lists keeps them strictly disjoint. The web3
       and signing primitives the helper calls are mocked, but the helper
       body — including the append site, the gas accounting math, and the
       caller-supplied list wiring — runs unmocked. This is the behavioural
       complement to the structural ``hasattr`` regression (which proves no
       instance-level list exists for two RPCs to share). Together they
       prove the cross-request leak is impossible by construction. NOTE
       (round-3 CodeRabbit thread): an earlier variant mocked
       ``_sign_and_submit_setup_tx`` itself with ``AsyncMock`` and only
       verified the mock; a regression in the real helper would still pass.
       That has been replaced — the new test mocks the layer BELOW the
       helper so the real helper executes end-to-end.

The tests stub web3 / CtfSDK so no real RPC is involved — the focus is on
the per-call ledger discipline, not on signing or chain interaction.
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from eth_account import Account

from almanak.framework.connectors.polymarket import TransactionData
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.polymarket_service import PolymarketServiceServicer

# Deterministic Anvil-style key — never funded, never used in production.
TEST_PRIVATE_KEY = "0x" + "ab" * 32
TEST_ACCOUNT = Account.from_key(TEST_PRIVATE_KEY)
TEST_WALLET = TEST_ACCOUNT.address


@pytest.fixture
def settings() -> MagicMock:
    s = MagicMock(spec=GatewaySettings)
    s.private_key = TEST_PRIVATE_KEY
    s.polymarket_private_key = None
    s.eoa_address = TEST_WALLET
    s.polymarket_wallet_address = None
    s.safe_address = None
    s.safe_mode = None
    s.polymarket_api_key = "k"
    s.polymarket_secret = "c2VjcmV0"  # base64("secret")
    s.polymarket_passphrase = "p"
    return s


@pytest.fixture
def servicer(settings: MagicMock) -> PolymarketServiceServicer:
    return PolymarketServiceServicer(settings=settings)


def _approval_tx(label: str) -> TransactionData:
    """Build a TransactionData stand-in. The data contents don't matter — the
    only thing under test is the per-call ledger that records gas spent."""
    return TransactionData(
        to="0x0000000000000000000000000000000000000001",
        data="0x",
        gas_estimate=60_000,
        description=label,
    )


def _record_for(tx_data: TransactionData) -> dict[str, Any]:
    """Build a record matching what _sign_and_submit_setup_tx would append
    to its caller-supplied list, so test stubs reproduce the production
    shape exactly."""
    return {
        "tx_hash": f"0x{abs(hash(tx_data.description)):064x}",
        "description": tx_data.description,
        "gas_used": int(tx_data.gas_estimate),
        "gas_price_wei": "50000000000",  # 50 gwei
        "total_cost_wei": str(tx_data.gas_estimate * 50_000_000_000),
    }


class _CtfStub:
    def __init__(self, *, approval_txs: list[TransactionData], pusd_balance: int = 0) -> None:
        self._approval_txs = approval_txs
        self._pusd_balance = pusd_balance
        self.source_asset = "0x" + "11" * 20
        self.native_usdc = "0x" + "33" * 20
        self.collateral_onramp = "0x" + "44" * 20

    def ensure_allowances(self, _wallet: str, _web3: Any) -> list[TransactionData]:
        return list(self._approval_txs)

    def get_pusd_balance(self, _wallet: str, _web3: Any) -> int:
        return self._pusd_balance

    def get_source_asset_balance(self, _wallet: str, _web3: Any) -> int:
        return 10_000_000_000

    def check_allowances(self, _wallet: str, _web3: Any):  # noqa: ANN201
        from almanak.framework.connectors.polymarket.ctf_sdk import (
            MAX_UINT256,
            AllowanceStatus,
        )

        return AllowanceStatus(
            source_asset_balance=10_000_000_000,
            pusd_balance=self._pusd_balance,
            source_asset_allowance_onramp=MAX_UINT256,
            pusd_allowance_ctf_exchange=MAX_UINT256,
            pusd_allowance_neg_risk_exchange=MAX_UINT256,
            pusd_allowance_neg_risk_adapter=MAX_UINT256,
            ctf_approved_for_ctf_exchange=True,
            ctf_approved_for_neg_risk_adapter=True,
            native_usdc_balance=0,
            native_usdc_allowance_onramp=0,
        )

    def select_source_for_wrap(self, _deficit: int, _status: Any) -> str:
        return self.source_asset

    def build_approve_collateral_tx(self, asset: str, _spender: str, _sender: str) -> TransactionData:
        return TransactionData(to=asset, data="0x", gas_estimate=80_000, description="approve")

    def build_wrap_to_pusd_tx(
        self,
        _wallet: str,
        amount: int,
        source_asset: str | None = None,  # noqa: ARG002
    ) -> TransactionData:
        return TransactionData(
            to="0x" + "22" * 20,
            data="0x",
            gas_estimate=150_000,
            description=f"wrap {amount}",
        )


def _fake_web3(block_number: int = 1_000) -> MagicMock:
    web3 = MagicMock()
    web3.eth.block_number = block_number
    return web3


# =============================================================================
# (g) _ensure_wallet_ready runs N setup txs -> returns list with N entries
# =============================================================================


class TestEnsureWalletReadyRecordsSetupTxs:
    """When the wallet needs N approvals, the returned list ends up with
    exactly N entries — one per approval submitted."""

    @pytest.mark.asyncio
    async def test_five_approvals_produce_five_entries(self, servicer: PolymarketServiceServicer) -> None:
        approvals = [_approval_tx(f"Approval #{i}") for i in range(5)]
        ctf = _CtfStub(approval_txs=approvals, pusd_balance=10_000_000)
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()

        # Stub the signer so we don't actually broadcast — but DO append to
        # the caller-supplied request-scoped list so the discipline is exercised.
        async def _fake_submit(tx_data: TransactionData, setup_txs: list[dict[str, Any]]) -> str:
            setup_txs.append(_record_for(tx_data))
            return "0xhash"

        servicer._sign_and_submit_setup_tx = AsyncMock(side_effect=_fake_submit)

        result = await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        # 5 approvals + 0 wrap (pUSD balance 10M >= min 5M, no wrap needed).
        assert len(result) == 5
        descriptions = [r["description"] for r in result]
        assert descriptions == [f"Approval #{i}" for i in range(5)]

    @pytest.mark.asyncio
    async def test_approvals_plus_wrap_records_six_entries(self, servicer: PolymarketServiceServicer) -> None:
        """5 approvals + 1 wrap = 6 entries when pUSD balance is short."""
        approvals = [_approval_tx(f"Approval #{i}") for i in range(5)]
        ctf = _CtfStub(approval_txs=approvals, pusd_balance=0)
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()

        async def _fake_submit(tx_data: TransactionData, setup_txs: list[dict[str, Any]]) -> str:
            setup_txs.append(_record_for(tx_data))
            return "0xhash"

        servicer._sign_and_submit_setup_tx = AsyncMock(side_effect=_fake_submit)

        result = await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        assert len(result) == 6  # 5 approvals + 1 wrap
        descriptions = [r["description"] for r in result]
        assert descriptions[:5] == [f"Approval #{i}" for i in range(5)]
        # Wrap description includes the deficit amount.
        assert descriptions[5].startswith("wrap")


# =============================================================================
# (h) _ensure_wallet_ready short-circuit -> empty list
# =============================================================================


class TestShortCircuitProducesEmptyLedger:
    """When allowances are already applied AND the wallet holds enough pUSD,
    no setup tx is submitted -> the returned list is empty."""

    @pytest.mark.asyncio
    async def test_allowances_applied_and_no_wrap_needed_records_nothing(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        ctf = _CtfStub(approval_txs=[], pusd_balance=10_000_000)
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()
        servicer._allowances_applied = True  # already done
        servicer._sign_and_submit_setup_tx = AsyncMock()

        result = await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        assert result == []
        servicer._sign_and_submit_setup_tx.assert_not_called()

    @pytest.mark.asyncio
    async def test_first_buy_with_no_approvals_or_wrap_records_nothing(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Edge case: first BUY but ensure_allowances returns []. Nothing to
        sign at all -> empty list."""
        ctf = _CtfStub(approval_txs=[], pusd_balance=10_000_000)
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()
        # _allowances_applied starts False but ensure_allowances returns nothing.
        servicer._sign_and_submit_setup_tx = AsyncMock()

        result = await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        assert result == []
        # No txs submitted -> the signer was never called.
        servicer._sign_and_submit_setup_tx.assert_not_called()
        # ALLOWANCES marker still flipped True for short-circuit on next call.
        assert servicer._allowances_applied is True


# =============================================================================
# (i) Per-call isolation — each invocation returns only its own setup txs
# =============================================================================


class TestPerCallIsolation:
    """Once an order's response is built, the ledger for that call is gone —
    the next call starts fresh."""

    @pytest.mark.asyncio
    async def test_two_consecutive_buys_return_independent_lists(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """First BUY records 5 setup txs; second BUY (allowances now applied,
        pUSD covered) records 0. Each call's returned list contains only the
        txs for that call — no leakage across calls."""
        approvals = [_approval_tx(f"Approval #{i}") for i in range(5)]
        ctf = _CtfStub(approval_txs=approvals, pusd_balance=10_000_000)
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()

        async def _fake_submit(tx_data: TransactionData, setup_txs: list[dict[str, Any]]) -> str:
            setup_txs.append(_record_for(tx_data))
            return "0xhash"

        servicer._sign_and_submit_setup_tx = AsyncMock(side_effect=_fake_submit)

        # FIRST BUY
        first_result = await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert len(first_result) == 5

        # SECOND BUY — allowances now applied, pUSD covered, no new submissions.
        second_result = await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert second_result == []
        # And the first list was not mutated by the second call.
        assert len(first_result) == 5

    @pytest.mark.asyncio
    async def test_no_instance_attribute_for_pending_setup_txs(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Regression for CodeRabbit thread 3: the instance-level
        ``_pending_setup_txs`` ledger MUST NOT exist anymore. Its presence
        would re-introduce the cross-request races the request-scoping fixed."""
        assert not hasattr(servicer, "_pending_setup_txs"), (
            "PolymarketServiceServicer must not carry an instance-level "
            "_pending_setup_txs — setup_txs are request-scoped only"
        )
        # Same regression on the drain helper — it had no purpose once the
        # ledger left the servicer, so it must be gone too.
        assert not hasattr(servicer, "_drain_pending_setup_txs")


# =============================================================================
# (i.5) CONCURRENT BUYs do not cross-contaminate (CodeRabbit thread 3)
# =============================================================================


class TestConcurrentSetupTxIsolation:
    """Regression guard for the shared-state race that motivated CodeRabbit
    thread 3 — exercising the REAL helper this time (round-3 fix).

    What is actually proved here:

    The REAL ``_sign_and_submit_setup_tx`` runs end-to-end under concurrent
    invocation. Web3 primitives (``get_transaction_count``,
    ``send_raw_transaction``, ``wait_for_transaction_receipt``) and crypto
    (``Account.sign_transaction``) are mocked so no network or real key is
    needed, but the helper body — the chain-id assertion call, the gas
    accounting math, the receipt extraction, and crucially the
    ``setup_txs.append(...)`` to the caller-supplied list — all execute
    unmocked. If a regression makes the helper append to
    ``self._pending_setup_txs`` (or any shared instance attribute) instead of
    the parameter list, this test fails: the two callers would observe each
    other's records.

    Why we do NOT drive ``_ensure_wallet_ready`` concurrently here: the
    method is serialised behind ``_wallet_ready_lock``, so two concurrent
    ``CreateAndPostOrder`` calls cannot run their bodies in parallel. The
    helper itself, on the other hand, is NOT lock-protected — concurrent
    callers from different RPC paths could in principle interleave on it,
    and that's precisely the contract this test pins down.

    Why we no longer mock ``_sign_and_submit_setup_tx`` itself: an earlier
    variant (round-2) replaced the helper with an ``AsyncMock`` whose
    ``side_effect`` did the appending. CodeRabbit (round 3) correctly
    flagged that this only verified the mock — a regression that broke the
    real helper's append wiring would still pass. Now we mock the layer
    BELOW the helper so the helper itself runs.

    Together with :class:`TestPerCallIsolation`'s ``hasattr`` regression
    (proving no instance-level list exists), these two guards make the
    cross-request leak impossible by construction:

      1. ``hasattr(servicer, "_pending_setup_txs") is False`` — there is no
         shared mutable state for two RPCs to race on
         (see :class:`TestPerCallIsolation`).
      2. The REAL append path in ``_sign_and_submit_setup_tx`` writes only
         to the destination list it received as an argument, so every
         caller's list is physically distinct (asserted below).
    """

    @pytest.mark.asyncio
    async def test_two_concurrent_calls_with_overlapping_setup_txs_stay_disjoint(
        self, servicer: PolymarketServiceServicer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Drive the REAL ``_sign_and_submit_setup_tx`` from two tasks with
        two distinct request-scoped lists. Web3 and signing primitives are
        mocked, but the helper body (append, gas math, list wiring) runs
        unmocked. ``wait_for_transaction_receipt`` yields control via
        ``asyncio.sleep(0)`` to force scheduler interleaving so that, if any
        shared state were lurking inside the helper, the two lists would
        cross-contaminate. Assert each list contains ONLY its own records.

        This exercises the real helper end-to-end; if a regression makes the
        helper append to a shared instance attribute instead of the
        parameter list, this test fails because both callers would see the
        same combined list. We bypass ``_ensure_wallet_ready`` (which
        serialises behind the wallet-ready lock) so the test exercises the
        exact contract the request-scoping fix introduced — the lock isn't
        the guard against cross-contamination, the parameter signature is.
        """
        # ----- Build a mock web3 the REAL helper can drive end-to-end. -----
        web3 = MagicMock()
        # to_checksum_address is called with tx_data.to before signing; pass-through
        # is fine since the test addresses are already lower-case 0x-prefixed.
        web3.to_checksum_address = lambda addr: addr
        # Nonce + chain_id pre-flight for the tx dict.
        web3.eth.get_transaction_count = MagicMock(return_value=42)
        web3.eth.chain_id = 137  # never observed (we mock _assert_polygon_chain_id)

        # send_raw_transaction returns bytes the helper renders as 0x-hex.
        # Distinct-per-call is unnecessary — the test asserts on description, not hash.
        send_calls: list[bytes] = []

        def _send_raw(raw: bytes) -> bytes:
            send_calls.append(raw)
            return b"\xab" * 32

        web3.eth.send_raw_transaction = MagicMock(side_effect=_send_raw)

        # The receipt provides gas accounting fields the helper extracts.
        # Crucially, wait_for_transaction_receipt is the await point inside
        # the helper that gives the asyncio scheduler a chance to interleave
        # the two tasks. ``asyncio.to_thread`` already yields, but we add a
        # ``sleep(0)`` inside the side_effect so interleaving is forced even
        # if the to_thread executor returns synchronously.
        # NOTE: ``asyncio.to_thread`` runs its callable in a thread, so the
        # callable itself must be sync. We force interleaving by yielding to
        # the loop INSIDE the helper via the get_block step in
        # _build_eip1559_gas_fields — but we mock that helper out, so instead
        # we replace ``asyncio.to_thread`` with an inline shim that performs
        # the call and then yields.
        receipt = MagicMock()
        receipt.status = 1
        receipt.gasUsed = 60_000
        receipt.effectiveGasPrice = 50_000_000_000  # 50 gwei

        def _wait_receipt(_tx_hash: bytes, _timeout: int) -> MagicMock:
            return receipt

        web3.eth.wait_for_transaction_receipt = MagicMock(side_effect=_wait_receipt)

        # Patch asyncio.to_thread used INSIDE the polymarket_service module so
        # every sync RPC call yields to the loop after running. This is the
        # primary interleaving lever for the test — without it, all of call
        # A's appends could run before call B even starts.
        #
        # Round-4 CodeRabbit fix: the shim itself is not enough. The earlier
        # version forced yields, but nothing in the test FAILED if a future
        # change made the helper effectively serial again. Now each yield is
        # recorded into a per-task trace, and the test asserts both tasks
        # reach a yield point BEFORE either finishes — making any regression
        # that drops the yields a hard test failure.
        original_to_thread = asyncio.to_thread
        trace: list[str] = []
        # Per-task identity propagated through the asyncio context so the
        # to_thread shim can tag yields without needing extra parameters.
        current_task_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "current_task_id",
            default=None,
        )

        async def _yielding_to_thread(func, *args, **kwargs):  # type: ignore[no-untyped-def]
            result = func(*args, **kwargs)
            tid = current_task_id.get()
            if tid is not None:
                trace.append(f"{tid}-yielded-pre")
            # Yield twice: once to let the other task pick up after the sync
            # call, once more so it has a chance to also reach its yield point.
            await asyncio.sleep(0)
            if tid is not None:
                trace.append(f"{tid}-yielded-post")
            await asyncio.sleep(0)
            return result

        monkeypatch.setattr(
            "almanak.gateway.services.polymarket_service.asyncio.to_thread",
            _yielding_to_thread,
        )

        # Bind the mock web3 to the servicer. Patching _get_polygon_web3 (and
        # not setting _polygon_web3 directly) confirms the helper goes through
        # its normal accessor path.
        servicer._get_polygon_web3 = MagicMock(return_value=web3)  # type: ignore[method-assign]
        # Chain-id assertion is an awaitable — no-op it.
        servicer._assert_polygon_chain_id = AsyncMock(return_value=None)  # type: ignore[method-assign]
        # Gas fields stub returns deterministic EIP-1559 values; the REAL
        # helper still uses these to populate the tx dict and feeds them into
        # the gas_price_wei fallback.
        servicer._build_eip1559_gas_fields = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "maxFeePerGas": 100_000_000_000,
                "maxPriorityFeePerGas": 30_000_000_000,
            }
        )
        # Throwaway test signer; the REAL helper checks both are truthy.
        servicer._wallet_address = TEST_WALLET
        servicer._private_key = TEST_PRIVATE_KEY

        # Mock Account.sign_transaction so we don't pay the EIP-1559 RLP +
        # secp256k1 cost for every test invocation. The REAL helper still
        # builds the tx dict, calls this, and forwards .raw_transaction to
        # send_raw_transaction.
        signed = MagicMock()
        signed.raw_transaction = b"\xde\xad\xbe\xef"

        # Each call records the tx dict it was handed so we can spot a
        # regression that mutates / shares the dict across calls.
        sign_calls: list[dict[str, Any]] = []

        def _fake_sign(tx: dict[str, Any], _key: str) -> MagicMock:
            sign_calls.append(dict(tx))
            return signed

        with patch(
            "almanak.gateway.services.polymarket_service.Account.sign_transaction",
            side_effect=_fake_sign,
        ):
            # Each call body owns its OWN list — that's exactly the
            # request-scoped contract _ensure_wallet_ready provides in
            # production. We invoke the REAL helper directly for fine-grained
            # interleaving control.
            a_list: list[dict[str, Any]] = []
            b_list: list[dict[str, Any]] = []
            a_txs = [_approval_tx(f"A-{i}") for i in range(3)]
            b_txs = [_approval_tx(f"B-{i}") for i in range(2)]

            async def _call_a() -> list[dict[str, Any]]:
                current_task_id.set("A")
                for tx_data in a_txs:
                    await servicer._sign_and_submit_setup_tx(tx_data, a_list)
                trace.append("A-finished")
                return a_list

            async def _call_b() -> list[dict[str, Any]]:
                current_task_id.set("B")
                for tx_data in b_txs:
                    await servicer._sign_and_submit_setup_tx(tx_data, b_list)
                trace.append("B-finished")
                return b_list

            a_result, b_result = await asyncio.gather(_call_a(), _call_b())

        # ----- Behavioural assertions on the REAL helper's output. -----
        # A submitted 3 txs, B submitted 2. Each call's list contains ONLY
        # its own — no cross-contamination. Pre-fix (or under a regression
        # that re-introduces a shared instance attribute) this would have
        # failed: a single shared list would have ended up with all 5 records
        # visible to both callers.
        assert len(a_result) == 3
        assert len(b_result) == 2
        # Every record in A's list starts with "A-"; every B record with "B-".
        assert all(r["description"].startswith("A-") for r in a_result)
        assert all(r["description"].startswith("B-") for r in b_result)
        # The lists are physically distinct objects too — mutating one must
        # never bleed into the other.
        assert a_result is not b_result

        # ----- Sanity checks proving the REAL helper actually executed. -----
        # 5 setup txs total (3 A + 2 B): each one drove send_raw + sign.
        assert len(send_calls) == 5
        assert len(sign_calls) == 5
        # Gas accounting math ran for real: gas_used * effectiveGasPrice.
        # 60_000 * 50_000_000_000 = 3_000_000_000_000_000 wei.
        for r in (*a_result, *b_result):
            assert r["gas_used"] == 60_000
            assert r["gas_price_wei"] == "50000000000"
            assert r["total_cost_wei"] == "3000000000000000"
            # tx_hash was rendered from the bytes returned by send_raw_transaction.
            assert r["tx_hash"].startswith("0x")
            # The hex string the helper renders is 64 chars (no 0x prefix).
            assert len(r["tx_hash"]) == 66

        # ----- And critically: scheduler interleaving DID occur. -----
        # CodeRabbit (round 4) flagged that the previous version of this test
        # only DOCUMENTED the interleaving — nothing failed if a refactor
        # silently made the helper serial again. The trace below makes the
        # concurrency contract a hard test assertion: both tasks must reach a
        # yield point before EITHER finishes.
        a_yield_indices = [i for i, m in enumerate(trace) if m.startswith("A-yielded")]
        b_yield_indices = [i for i, m in enumerate(trace) if m.startswith("B-yielded")]
        assert a_yield_indices, f"A never recorded a yield marker; trace={trace!r}"
        assert b_yield_indices, f"B never recorded a yield marker; trace={trace!r}"

        a_finished_idx = trace.index("A-finished")
        b_finished_idx = trace.index("B-finished")
        # Each task must yield at least once before its own finish.
        assert a_yield_indices[0] < a_finished_idx, (
            f"A finished without yielding first; trace={trace!r}"
        )
        assert b_yield_indices[0] < b_finished_idx, (
            f"B finished without yielding first; trace={trace!r}"
        )
        # Strong overlap claim: BOTH tasks must reach a yield BEFORE EITHER
        # finishes. If a regression makes A run to completion before B even
        # starts (or vice-versa), the losing task's first yield will land
        # AFTER the first finish marker and this assertion fires.
        first_finish = min(a_finished_idx, b_finished_idx)
        assert a_yield_indices[0] < first_finish, (
            f"A did not yield before any task finished; trace={trace!r}"
        )
        assert b_yield_indices[0] < first_finish, (
            f"B did not yield before any task finished; trace={trace!r}"
        )

        # Restore the real to_thread for any later tests in the same module.
        monkeypatch.setattr(
            "almanak.gateway.services.polymarket_service.asyncio.to_thread",
            original_to_thread,
        )
