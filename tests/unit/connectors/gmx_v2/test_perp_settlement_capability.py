"""VIB-3872 WI-2 — RunnerPerpSettlementCapability + GMX correlation.

The verdict-contract tests are mutation-resistant: each BINDING per-state clause
(design D1) is exercised so deleting the guard in ``__post_init__`` fails a test.

The GMX-impl tests drive :func:`resolve_perp_settlements` through a FAKE
gateway-web3 handle (no sockets) whose ``get_logs`` / ``get_transaction_receipt``
replay REAL Arbitrum keeper bytes from the WI-1 fixture — so the EXECUTED path
proves the full correlation (order_key → OrderExecuted log → keeper receipt →
measured ``PerpFillData``) end-to-end.
"""

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors._strategy_base.runner_hook_registry import (
    PerpSettlementState,
    PerpSettlementVerdict,
    PerpSettlementWatchEntry,
    RunnerHookRegistry,
    RunnerPerpSettlementCapability,
)
from almanak.connectors.gmx_v2 import perp_settlement as ps
from almanak.connectors.gmx_v2.receipt_parser import EVENT_TOPICS, PerpFillData
from almanak.connectors.gmx_v2.runner_hooks import GmxV2RunnerHookConnector
from tests.unit.connectors.gmx_v2.market_fixtures import prime_catalog


# Settlement price scaling reads the venue-verified catalog (address-first);
# prime the audited fixture snapshot, standing in for the compile-time dynamic
# verification that precedes any settlement in a live process.
@pytest.fixture(autouse=True)
def _verified_markets():
    prime_catalog()


_FIXTURE = Path(__file__).parent / "fixtures" / "gmx_keeper_receipts_arbitrum.json"
_OPEN_ORDER_KEY = "0x585d42d95b9a4e84e78d53073d85fa6e67304c119fc000a6052661068200f9cf"
_EMITTER = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"


@pytest.fixture(scope="module")
def receipts() -> dict:
    return json.loads(_FIXTURE.read_text())


# --------------------------------------------------------------------------- #
# Verdict per-state contract (mutation-resistant)
# --------------------------------------------------------------------------- #


class TestVerdictContract:
    def test_executed_requires_fill_data(self) -> None:
        with pytest.raises(ValueError, match="EXECUTED must carry fill_data"):
            PerpSettlementVerdict(order_key="0xk", state=PerpSettlementState.EXECUTED, terminal=True)

    def test_executed_with_fill_ok(self) -> None:
        v = PerpSettlementVerdict(
            order_key="0xk", state=PerpSettlementState.EXECUTED, terminal=True, fill_data=PerpFillData()
        )
        assert v.state is PerpSettlementState.EXECUTED

    @pytest.mark.parametrize("state", [PerpSettlementState.NOT_FOUND_UNCORRELATED, PerpSettlementState.UNMEASURED])
    def test_unmeasured_states_forbid_fill_data(self, state: PerpSettlementState) -> None:
        with pytest.raises(ValueError, match="must NOT carry fill_data"):
            PerpSettlementVerdict(
                order_key="0xk", state=state, terminal=True, fill_data=PerpFillData(), unavailable_reason="x"
            )

    @pytest.mark.parametrize("state", [PerpSettlementState.NOT_FOUND_UNCORRELATED, PerpSettlementState.UNMEASURED])
    def test_unmeasured_states_require_reason(self, state: PerpSettlementState) -> None:
        with pytest.raises(ValueError, match="must carry unavailable_reason"):
            PerpSettlementVerdict(order_key="0xk", state=state, terminal=True)

    def test_cancelled_may_omit_fill_data(self) -> None:
        v = PerpSettlementVerdict(order_key="0xk", state=PerpSettlementState.CANCELLED, terminal=True)
        assert v.fill_data is None

    def test_cancelled_may_carry_fill_data(self) -> None:
        v = PerpSettlementVerdict(
            order_key="0xk", state=PerpSettlementState.CANCELLED, terminal=True, fill_data=PerpFillData()
        )
        assert v.fill_data is not None

    @pytest.mark.parametrize(
        "state",
        [
            PerpSettlementState.EXECUTED,
            PerpSettlementState.CANCELLED,
            PerpSettlementState.FROZEN,
            PerpSettlementState.NOT_FOUND_UNCORRELATED,
        ],
    )
    def test_measured_terminal_states_reject_non_terminal(self, state: PerpSettlementState) -> None:
        """EXECUTED / CANCELLED / FROZEN / NOT_FOUND_UNCORRELATED are terminal-only —
        each guard is exercised separately so dropping one fails exactly its case."""
        kwargs: dict = {"order_key": "0xk", "state": state, "terminal": False}
        if state is PerpSettlementState.EXECUTED:
            kwargs["fill_data"] = PerpFillData()
        if state is PerpSettlementState.NOT_FOUND_UNCORRELATED:
            kwargs["unavailable_reason"] = "x"
        with pytest.raises(ValueError, match=f"{state.value} must be terminal"):
            PerpSettlementVerdict(**kwargs)

    @pytest.mark.parametrize("state", [PerpSettlementState.CANCELLED, PerpSettlementState.FROZEN])
    def test_cancelled_frozen_terminal_true_ok(self, state: PerpSettlementState) -> None:
        assert PerpSettlementVerdict(order_key="0xk", state=state, terminal=True).terminal is True

    def test_pending_must_be_non_terminal_and_no_fill(self) -> None:
        assert (
            PerpSettlementVerdict(order_key="0xk", state=PerpSettlementState.PENDING, terminal=False).terminal is False
        )
        with pytest.raises(ValueError, match="PENDING must be non-terminal"):
            PerpSettlementVerdict(order_key="0xk", state=PerpSettlementState.PENDING, terminal=True)
        with pytest.raises(ValueError, match="PENDING must NOT carry fill_data"):
            PerpSettlementVerdict(
                order_key="0xk", state=PerpSettlementState.PENDING, terminal=False, fill_data=PerpFillData()
            )

    def test_to_dict_serializes_fill(self) -> None:
        v = PerpSettlementVerdict(
            order_key="0xk",
            state=PerpSettlementState.EXECUTED,
            terminal=True,
            fill_data=PerpFillData(size_delta_usd=Decimal("5")),
            keeper_tx_hash="0xtx",
        )
        d = v.to_dict()
        assert d["state"] == "EXECUTED"
        assert d["fill_data"]["size_delta_usd"] == "5"
        assert d["keeper_tx_hash"] == "0xtx"


# --------------------------------------------------------------------------- #
# Capability registration
# --------------------------------------------------------------------------- #


class TestCapabilityRegistration:
    def test_connector_implements_capability(self) -> None:
        assert isinstance(GmxV2RunnerHookConnector(), RunnerPerpSettlementCapability)

    def test_registry_accepts_and_dispatches(self) -> None:
        reg = RunnerHookRegistry()
        reg.register(GmxV2RunnerHookConnector())
        assert reg.perp_settlement_policy("gmx_v2").timeout_seconds == ps.PERP_SETTLEMENT_TIMEOUT_SECONDS
        # No gateway → fail-closed empty/None handling, never a raise.
        out = reg.resolve_perp_settlements(
            protocol="gmx_v2", gateway_client=None, chain="arbitrum", wallet_address="0x", watch_entries=()
        )
        assert out == ()

    def test_registry_unknown_protocol_returns_none(self) -> None:
        reg = RunnerHookRegistry()
        reg.register(GmxV2RunnerHookConnector())
        assert (
            reg.resolve_perp_settlements(
                protocol="nope", gateway_client=object(), chain="arbitrum", wallet_address="0x", watch_entries=()
            )
            is None
        )


# --------------------------------------------------------------------------- #
# GMX correlation via a fake gateway-web3 handle (real keeper bytes)
# --------------------------------------------------------------------------- #


class _FakeEth:
    def __init__(self, logs_result=None, logs_error=None, receipts_by_tx=None, receipt_error=None):
        self._logs_result = logs_result if logs_result is not None else []
        self._logs_error = logs_error
        self._receipts = receipts_by_tx or {}
        self._receipt_error = receipt_error
        self.get_logs_calls: list[dict] = []

    def get_logs(self, filt):
        self.get_logs_calls.append(filt)
        if self._logs_error is not None:
            raise self._logs_error
        # Emulate server-side indexed-topic filtering: only return logs whose
        # topic[2] matches the requested order key (topics[2] in the filter).
        wanted = (filt.get("topics") or [None, None, None])[2]
        if wanted is None:
            return list(self._logs_result)
        return [lg for lg in self._logs_result if str(lg["topics"][2]).lower() == str(wanted).lower()]

    def get_transaction_receipt(self, tx_hash):
        if self._receipt_error is not None:
            raise self._receipt_error
        return self._receipts[str(tx_hash)]


class _FakeWeb3:
    def __init__(self, eth):
        self.eth = eth


def _order_log(event_name: str, order_key: str, tx_hash: str) -> dict:
    return {
        "address": _EMITTER,
        "topics": ["0x" + "11" * 32, EVENT_TOPICS[event_name], order_key],
        "data": "0x",
        "transactionHash": tx_hash,
        "logIndex": 3,
    }


def _pending(order_keys, ok=True, truncated=False, error=None):
    return SimpleNamespace(order_keys=list(order_keys), orders=[], ok=ok, truncated=truncated, error=error)


def _patch(monkeypatch, *, eth, pending_result=None):
    monkeypatch.setattr(ps, "get_gateway_web3", lambda gc, chain: _FakeWeb3(eth))
    if pending_result is not None:
        monkeypatch.setattr(ps, "read_pending_orders", lambda gc, chain, wallet: pending_result)


def _entry(seconds=10, block=487_783_560) -> PerpSettlementWatchEntry:
    return PerpSettlementWatchEntry(
        order_key=_OPEN_ORDER_KEY, submission_block=block, is_open=True, seconds_since_submission=seconds
    )


def _resolve(monkeypatch, entry):
    return ps.resolve_perp_settlements(
        gateway_client=object(), chain="arbitrum", wallet_address="0xwallet", watch_entries=(entry,)
    )[0]


class TestGmxCorrelation:
    def test_executed_returns_measured_fill(self, monkeypatch, receipts) -> None:
        tx = "0x" + receipts["_provenance"]["open_tx"]
        eth = _FakeEth(
            logs_result=[_order_log("OrderExecuted", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: receipts["open"]},
        )
        _patch(monkeypatch, eth=eth)
        v = _resolve(monkeypatch, _entry())

        assert v.state is PerpSettlementState.EXECUTED
        assert v.terminal is True
        assert v.keeper_tx_hash == tx
        assert isinstance(v.fill_data, PerpFillData)
        assert v.fill_data.is_open is True
        assert v.fill_data.entry_price is not None and v.fill_data.entry_price > 0
        assert v.fill_data.size_delta_usd is not None and v.fill_data.size_delta_usd > 0
        assert v.fill_data.position_fee_usd is not None
        # The eth_getLogs scan is topic-indexed by the order key over [block, latest].
        filt = eth.get_logs_calls[0]
        assert filt["fromBlock"] == _entry().submission_block
        assert filt["toBlock"] == "latest"
        assert filt["topics"][2] == _OPEN_ORDER_KEY

    def test_unlisted_market_fill_uses_gateway_verified_decimals(self, monkeypatch, receipts) -> None:
        parsed = ps.GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipts["close"])
        assert parsed is not None and parsed.order_key and parsed.exit_price is None
        tx = "0x" + receipts["_provenance"]["close_tx"]
        eth = _FakeEth(
            logs_result=[_order_log("OrderExecuted", parsed.order_key, tx)],
            receipts_by_tx={tx: receipts["close"]},
        )
        _patch(monkeypatch, eth=eth)
        resolve_market = Mock(return_value=SimpleNamespace(index_token_decimals=18))
        monkeypatch.setattr(ps, "resolve_market_via_gateway", resolve_market)

        gateway_client = object()
        verdict = ps.resolve_perp_settlements(
            gateway_client=gateway_client,
            chain="arbitrum",
            wallet_address="0xwallet",
            watch_entries=(
                PerpSettlementWatchEntry(
                    order_key=parsed.order_key,
                    submission_block=1,
                    is_open=False,
                    seconds_since_submission=10,
                ),
            ),
        )[0]

        assert verdict.state is PerpSettlementState.EXECUTED
        assert verdict.fill_data is not None
        assert verdict.fill_data.exit_price == Decimal("0.144104694440")
        resolve_market.assert_called_once_with(gateway_client, chain="arbitrum", market=parsed.market)

    def test_cancelled_without_position_event_has_no_fill(self, monkeypatch, receipts) -> None:
        tx = "0xcancel"
        # A cancel keeper tx carries no PositionIncrease/Decrease → extract_perp_fill None.
        cancel_receipt = {"transactionHash": tx, "blockNumber": 1, "status": 1, "logs": []}
        eth = _FakeEth(
            logs_result=[_order_log("OrderCancelled", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: cancel_receipt},
        )
        _patch(monkeypatch, eth=eth)
        v = _resolve(monkeypatch, _entry())
        assert v.state is PerpSettlementState.CANCELLED
        assert v.terminal is True
        assert v.fill_data is None

    def test_executed_without_correlated_fill_respects_the_horizon(self, monkeypatch) -> None:
        """VIB-6110: OrderExecuted whose keeper receipt yields no matching fill must EXPIRE.

        Correlating extraction to the watched order key tightened the condition
        that reaches this branch — previously any decodable position event
        satisfied it, now only an orderKey-matching one does. Left unconditionally
        non-terminal, a GMX payload-key rename or a forged-emitter receipt would
        re-poll this entry (one eth_getLogs + one eth_getTransactionReceipt per
        tick) forever and never book the PERP_SETTLEMENT row.
        """
        tx = "0xexec-no-fill"
        # OrderExecuted observed, but the keeper receipt carries no position event
        # attributable to our order key.
        empty_receipt = {"transactionHash": tx, "blockNumber": 1, "status": 1, "logs": []}

        eth = _FakeEth(
            logs_result=[_order_log("OrderExecuted", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: empty_receipt},
        )
        _patch(monkeypatch, eth=eth)
        within = _resolve(monkeypatch, _entry(seconds=10))
        assert within.state is PerpSettlementState.UNMEASURED
        assert within.terminal is False  # still inside the horizon → retry
        assert "fill economics unmeasured" in (within.unavailable_reason or "")

        eth = _FakeEth(
            logs_result=[_order_log("OrderExecuted", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: empty_receipt},
        )
        _patch(monkeypatch, eth=eth)
        expired = _resolve(monkeypatch, _entry(seconds=ps.PERP_SETTLEMENT_TIMEOUT_SECONDS + 1))
        assert expired.state is PerpSettlementState.UNMEASURED
        assert expired.terminal is True  # horizon passed → book terminal, stop polling
        assert expired.fill_data is None

    def test_frozen_is_terminal(self, monkeypatch) -> None:
        tx = "0xfrozen"
        eth = _FakeEth(
            logs_result=[_order_log("OrderFrozen", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: {"transactionHash": tx, "blockNumber": 1, "status": 1, "logs": []}},
        )
        _patch(monkeypatch, eth=eth)
        v = _resolve(monkeypatch, _entry())
        assert v.state is PerpSettlementState.FROZEN
        assert v.terminal is True

    def test_pending_when_still_in_orderbook_within_horizon(self, monkeypatch) -> None:
        eth = _FakeEth(logs_result=[])
        _patch(monkeypatch, eth=eth, pending_result=_pending([_OPEN_ORDER_KEY]))
        v = _resolve(monkeypatch, _entry(seconds=10))
        assert v.state is PerpSettlementState.PENDING
        assert v.terminal is False
        assert v.fill_data is None

    def test_horizon_expiry_is_unmeasured_not_cancelled(self, monkeypatch) -> None:
        eth = _FakeEth(logs_result=[])
        _patch(monkeypatch, eth=eth, pending_result=_pending([_OPEN_ORDER_KEY]))
        v = _resolve(monkeypatch, _entry(seconds=ps.PERP_SETTLEMENT_TIMEOUT_SECONDS + 1))
        assert v.state is PerpSettlementState.UNMEASURED  # never a fabricated CANCELLED
        assert v.terminal is True
        assert "horizon expired" in (v.unavailable_reason or "")

    def test_gone_from_orderbook_uncorrelated(self, monkeypatch) -> None:
        eth = _FakeEth(logs_result=[])
        _patch(monkeypatch, eth=eth, pending_result=_pending([], truncated=False))
        v = _resolve(monkeypatch, _entry(seconds=10))
        assert v.state is PerpSettlementState.NOT_FOUND_UNCORRELATED
        assert v.terminal is True
        assert v.unavailable_reason

    def test_truncated_pending_set_is_unmeasured(self, monkeypatch) -> None:
        eth = _FakeEth(logs_result=[])
        _patch(monkeypatch, eth=eth, pending_result=_pending([], truncated=True))
        v = _resolve(monkeypatch, _entry(seconds=10))
        assert v.state is PerpSettlementState.UNMEASURED
        assert v.terminal is False  # not expired → retry
        assert "truncated" in (v.unavailable_reason or "")

    def test_getlogs_failure_is_unmeasured_retry(self, monkeypatch) -> None:
        eth = _FakeEth(logs_error=RuntimeError("rpc down"))
        _patch(monkeypatch, eth=eth)
        v = _resolve(monkeypatch, _entry(seconds=10))
        assert v.state is PerpSettlementState.UNMEASURED
        assert v.terminal is False
        assert "eth_getLogs failed" in (v.unavailable_reason or "")

    def test_getlogs_failure_past_horizon_is_terminal(self, monkeypatch) -> None:
        eth = _FakeEth(logs_error=RuntimeError("rpc down"))
        _patch(monkeypatch, eth=eth)
        v = _resolve(monkeypatch, _entry(seconds=ps.PERP_SETTLEMENT_TIMEOUT_SECONDS + 5))
        assert v.state is PerpSettlementState.UNMEASURED
        assert v.terminal is True

    def test_no_gateway_client_is_unmeasured(self) -> None:
        v = ps.resolve_perp_settlements(
            gateway_client=None, chain="arbitrum", wallet_address="0x", watch_entries=(_entry(),)
        )[0]
        assert v.state is PerpSettlementState.UNMEASURED
        assert "gateway" in (v.unavailable_reason or "").lower()

    def test_unknown_chain_has_no_emitter(self) -> None:
        v = ps.resolve_perp_settlements(
            gateway_client=object(), chain="fantom", wallet_address="0x", watch_entries=(_entry(),)
        )[0]
        assert v.state is PerpSettlementState.UNMEASURED
        assert "EventEmitter" in (v.unavailable_reason or "")

    def test_malformed_order_key_is_unmeasured(self, monkeypatch) -> None:
        eth = _FakeEth(logs_result=[])
        _patch(monkeypatch, eth=eth)
        bad = PerpSettlementWatchEntry(order_key="0x1234", submission_block=1, seconds_since_submission=1)
        v = ps.resolve_perp_settlements(
            gateway_client=object(), chain="arbitrum", wallet_address="0x", watch_entries=(bad,)
        )[0]
        assert v.state is PerpSettlementState.UNMEASURED
        assert "malformed order key" in (v.unavailable_reason or "")

    def test_multi_handle_order_independent(self, monkeypatch, receipts) -> None:
        tx = "0x" + receipts["_provenance"]["open_tx"]
        eth = _FakeEth(
            logs_result=[_order_log("OrderExecuted", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: receipts["open"]},
        )
        _patch(monkeypatch, eth=eth, pending_result=_pending([]))
        # One executed (has logs), one with a different key that finds no outcome.
        other = PerpSettlementWatchEntry(order_key="0x" + "77" * 32, submission_block=1, seconds_since_submission=5)
        out = ps.resolve_perp_settlements(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address="0x",
            watch_entries=(_entry(), other),
        )
        assert len(out) == 2
        assert out[0].state is PerpSettlementState.EXECUTED
        # Second key isn't in the executed log set → gone/uncorrelated.
        assert out[1].state is PerpSettlementState.NOT_FOUND_UNCORRELATED

    def test_pending_read_is_batched_once_per_invocation(self, monkeypatch) -> None:
        """N no-outcome entries must trigger exactly ONE read_pending_orders read."""
        calls = {"n": 0}
        other = "0x" + "99" * 32

        def _counting_read(gc, chain, wallet):
            calls["n"] += 1
            return _pending([_OPEN_ORDER_KEY, other])

        eth = _FakeEth(logs_result=[])  # no outcome logs → both hit the pending path
        monkeypatch.setattr(ps, "get_gateway_web3", lambda gc, chain: _FakeWeb3(eth))
        monkeypatch.setattr(ps, "read_pending_orders", _counting_read)

        e2 = PerpSettlementWatchEntry(order_key=other, submission_block=1, seconds_since_submission=10)
        out = ps.resolve_perp_settlements(
            gateway_client=object(), chain="arbitrum", wallet_address="0x", watch_entries=(_entry(seconds=10), e2)
        )
        assert calls["n"] == 1
        assert all(v.state is PerpSettlementState.PENDING for v in out)

    def test_pending_read_skipped_when_every_entry_has_outcome(self, monkeypatch, receipts) -> None:
        """The pending read is lazy: an all-executed batch never reads the orderbook."""
        calls = {"n": 0}

        def _counting_read(gc, chain, wallet):
            calls["n"] += 1
            return _pending([])

        tx = "0x" + receipts["_provenance"]["open_tx"]
        eth = _FakeEth(
            logs_result=[_order_log("OrderExecuted", _OPEN_ORDER_KEY, tx)],
            receipts_by_tx={tx: receipts["open"]},
        )
        monkeypatch.setattr(ps, "get_gateway_web3", lambda gc, chain: _FakeWeb3(eth))
        monkeypatch.setattr(ps, "read_pending_orders", _counting_read)
        out = _resolve(monkeypatch, _entry())
        assert out.state is PerpSettlementState.EXECUTED
        assert calls["n"] == 0


class TestIntentTypeNormalization:
    """Fix for the string-backed intent_type receipt-threading gap (WI-2 review)."""

    def test_enum_backed_intent_type(self) -> None:
        from almanak.connectors.gmx_v2.runner_hooks import _intent_type_str

        assert _intent_type_str(SimpleNamespace(intent_type=SimpleNamespace(value="PERP_CLOSE"))) == "PERP_CLOSE"

    def test_string_backed_intent_type(self) -> None:
        from almanak.connectors.gmx_v2.runner_hooks import _intent_type_str

        # Previously yielded "" (no .value) → keeper receipts were silently dropped.
        assert _intent_type_str(SimpleNamespace(intent_type="PERP_OPEN")) == "PERP_OPEN"

    def test_missing_intent_type(self) -> None:
        from almanak.connectors.gmx_v2.runner_hooks import _intent_type_str

        assert _intent_type_str(SimpleNamespace()) == ""
