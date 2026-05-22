"""Characterization tests for ``build_verbose_revert_report`` and the
``VerboseRevertReport.format()`` output.

These tests pin the observable behaviour of the verbose operator-facing revert
report BEFORE refactoring (Phase 9.5) and MUST remain green after. Operator
dashboards / alerting scrape specific headers and keys out of the text report,
so we assert exact lines, ordering, and byte-for-byte formatting where
feasible.

Phase map covered:

- Header / execution context block (EXECUTION CONTEXT, timing).
- INTENT DETAILS block (None case, Pydantic ``model_dump``, plain ``__dict__``,
  no-dunder fallback, enum-valued ``intent_type``, id / intent_id fallback,
  underscore-prefixed params skipped, long-value truncation at 80 chars).
- ACTIONS block (enum vs string ``type``/``protocol``, params via
  ``model_dump``, via ``__dict__`` with private keys stripped, raw fallback,
  missing params, truncation).
- TRANSACTIONS block (bundle_tx_dicts pulled via ``tx_dict`` only when set,
  ``to``/``to_address`` fallback on receipt, ``gas`` vs ``gasLimit`` fallback,
  ``data`` vs ``input`` fallback, calldata decode of known + unknown selectors,
  empty calldata path, gas_used None → "N/A" line, missing bundle tx_dicts →
  zero defaults).
- RAW ERROR block (None omits section, >500 chars truncates, ≤500 preserved).
- Builder argument wiring: ``started_at`` default vs supplied, phase constant,
  context ``getattr`` fallbacks ("unknown"/""), bundle missing ``actions`` /
  ``transactions`` attrs → empty lists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from almanak.framework.execution.revert_diagnostics import (
    ActionDetails,
    IntentDetails,
    TransactionDetails,
    VerboseRevertReport,
    build_verbose_revert_report,
    decode_calldata_selector,
)

# ---------------------------------------------------------------------------
# Test doubles (minimal, local, no framework imports beyond revert_diagnostics)
# ---------------------------------------------------------------------------


class _EnumLike:
    """Minimal stand-in for an enum with a ``.value`` attribute."""

    def __init__(self, value: str) -> None:
        self.value = value


@dataclass
class _FakeContext:
    deployment_id: str = "strat-1"
    chain: str = "arbitrum"
    wallet_address: str = "0xWALLET"
    correlation_id: str = "corr-123"
    intent_description: str = "swap 1 WETH -> USDC"


@dataclass
class _FakeAction:
    type: Any = "SWAP"
    protocol: Any = "uniswap_v3"
    params: Any = None


@dataclass
class _FakeBundle:
    actions: list[Any] = field(default_factory=list)
    transactions: list[Any] = field(default_factory=list)
    intent: Any | None = None


@dataclass
class _FakeBundleTx:
    tx_dict: dict[str, Any] | None = None


@dataclass
class _FakeReceipt:
    to_address: str | None = None
    to: str | None = None


@dataclass
class _FakeTxResult:
    tx_hash: str = "0xTX"
    success: bool = False
    gas_used: int | None = 12345
    error: str | None = "STF"
    receipt: Any | None = None


# Fixed UTC timestamps so format() is deterministic.
STARTED_AT = datetime(2026, 4, 23, 10, 0, 0, tzinfo=UTC)
FAILED_AT_FROZEN = datetime(2026, 4, 23, 10, 0, 5, tzinfo=UTC)


def _freeze_failed_at(monkeypatch) -> None:
    """Pin ``datetime.now(UTC)`` inside the module to a fixed value so the
    ``failed_at`` timestamp is deterministic for golden-file assertions.
    """
    import almanak.framework.execution.revert_diagnostics as rd

    class _FrozenDT(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ARG003 - signature must match
            return FAILED_AT_FROZEN

    monkeypatch.setattr(rd, "datetime", _FrozenDT)


# ---------------------------------------------------------------------------
# build_verbose_revert_report — argument wiring
# ---------------------------------------------------------------------------


class TestBuildContextWiring:
    def test_context_fields_are_copied_verbatim(self):
        ctx = _FakeContext()
        report = build_verbose_revert_report(
            context=ctx,
            action_bundle=_FakeBundle(),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.deployment_id == "strat-1"
        assert report.chain == "arbitrum"
        assert report.wallet_address == "0xWALLET"
        assert report.correlation_id == "corr-123"
        assert report.intent_description == "swap 1 WETH -> USDC"

    def test_missing_context_fields_fall_back_to_unknown_and_empty(self):
        class _EmptyCtx:
            pass

        report = build_verbose_revert_report(
            context=_EmptyCtx(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.deployment_id == "unknown"
        assert report.chain == "unknown"
        assert report.wallet_address == "unknown"
        assert report.correlation_id == ""
        assert report.intent_description == ""

    def test_execution_phase_is_constant_confirmation(self):
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.execution_phase == "CONFIRMATION"

    def test_started_at_defaults_to_now_when_not_supplied(self, monkeypatch):
        _freeze_failed_at(monkeypatch)
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
        )
        assert report.started_at == FAILED_AT_FROZEN
        assert report.failed_at == FAILED_AT_FROZEN

    def test_failed_at_is_set_to_now_even_when_started_at_supplied(self, monkeypatch):
        _freeze_failed_at(monkeypatch)
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.started_at == STARTED_AT
        assert report.failed_at == FAILED_AT_FROZEN

    def test_raw_error_passthrough(self):
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            raw_error="some-error",
            started_at=STARTED_AT,
        )
        assert report.raw_error == "some-error"


# ---------------------------------------------------------------------------
# build_verbose_revert_report — intent details extraction
# ---------------------------------------------------------------------------


class TestBuildIntentDetails:
    def test_intent_none_produces_none_block(self):
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            intent=None,
            started_at=STARTED_AT,
        )
        assert report.intent is None

    def test_intent_with_model_dump_prefers_model_dump(self):
        class _PydIntent:
            intent_type = _EnumLike("SWAP")
            id = "intent-id-1"

            def model_dump(self) -> dict[str, Any]:
                return {"from_token": "WETH", "to_token": "USDC", "amount": "1"}

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            intent=_PydIntent(),
            started_at=STARTED_AT,
        )
        assert report.intent is not None
        assert report.intent.intent_type == "SWAP"
        assert report.intent.intent_id == "intent-id-1"
        assert report.intent.params == {"from_token": "WETH", "to_token": "USDC", "amount": "1"}

    def test_intent_without_model_dump_uses_dict_stripping_dunders(self):
        class _PlainIntent:
            def __init__(self) -> None:
                self.intent_type = "SWAP"  # plain string, no .value
                self.intent_id = "intent-id-2"
                self.from_token = "WETH"
                self._private = "should-not-appear"

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            intent=_PlainIntent(),
            started_at=STARTED_AT,
        )
        assert report.intent is not None
        assert report.intent.intent_type == "SWAP"
        assert report.intent.intent_id == "intent-id-2"
        assert "_private" not in report.intent.params
        assert report.intent.params.get("from_token") == "WETH"

    def test_intent_without_dict_yields_empty_params(self):
        # Use slots-only to force ``hasattr(intent, "__dict__") is False``.
        class _Slotted:
            __slots__ = ()

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            intent=_Slotted(),
            started_at=STARTED_AT,
        )
        assert report.intent is not None
        assert report.intent.intent_type == "UNKNOWN"
        assert report.intent.intent_id == ""
        assert report.intent.params == {}

    def test_intent_id_falls_back_to_intent_id_when_id_absent(self):
        class _IntentIdOnly:
            intent_type = "SWAP"
            intent_id = "fallback-id"

            def model_dump(self) -> dict[str, Any]:
                return {}

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            intent=_IntentIdOnly(),
            started_at=STARTED_AT,
        )
        assert report.intent is not None
        assert report.intent.intent_id == "fallback-id"


# ---------------------------------------------------------------------------
# build_verbose_revert_report — action details extraction
# ---------------------------------------------------------------------------


class TestBuildActionDetails:
    def test_bundle_without_actions_attr_yields_empty(self):
        class _NoActions:
            transactions: list[Any] = []

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_NoActions(),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.actions == []

    def test_action_type_and_protocol_enum_value_unwrapped(self):
        action = _FakeAction(type=_EnumLike("SWAP"), protocol=_EnumLike("uniswap_v3"))
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(actions=[action]),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert len(report.actions) == 1
        assert report.actions[0].action_type == "SWAP"
        assert report.actions[0].protocol == "uniswap_v3"

    def test_action_type_and_protocol_plain_string_passthrough(self):
        action = _FakeAction(type="SWAP", protocol="uniswap_v3")
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(actions=[action]),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.actions[0].action_type == "SWAP"
        assert report.actions[0].protocol == "uniswap_v3"

    def test_action_params_model_dump_is_preferred(self):
        class _PydParams:
            def model_dump(self) -> dict[str, Any]:
                return {"a": 1, "b": 2}

        action = _FakeAction(params=_PydParams())
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(actions=[action]),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.actions[0].params == {"a": 1, "b": 2}

    def test_action_params_dict_strips_private_keys(self):
        class _PlainParams:
            def __init__(self) -> None:
                self.a = 1
                self._hidden = 2

        action = _FakeAction(params=_PlainParams())
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(actions=[action]),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.actions[0].params == {"a": 1}

    def test_action_params_none_yields_empty_dict(self):
        action = _FakeAction(params=None)
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(actions=[action]),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.actions[0].params == {}

    def test_action_params_raw_fallback_uses_str_repr(self):
        class _Opaque:
            __slots__ = ()

            def __str__(self) -> str:
                return "opaque-value"

        action = _FakeAction(params=_Opaque())
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(actions=[action]),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.actions[0].params == {"raw": "opaque-value"}


# ---------------------------------------------------------------------------
# build_verbose_revert_report — transaction details extraction
# ---------------------------------------------------------------------------


class TestBuildTransactionDetails:
    def test_no_transaction_results_yields_empty_list(self):
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[],
            started_at=STARTED_AT,
        )
        assert report.transactions == []

    def test_bundle_without_transactions_still_includes_tr_details(self):
        """When bundle lacks ``transactions`` attr, we still emit per-tr
        entries with defaulted calldata/value/gas/nonce fields."""

        class _BundleNoTx:
            actions: list[Any] = []

        tr = _FakeTxResult(tx_hash="0xabc", success=False, gas_used=10)
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_BundleNoTx(),
            transaction_results=[tr],
            started_at=STARTED_AT,
        )
        assert len(report.transactions) == 1
        t = report.transactions[0]
        assert t.tx_hash == "0xabc"
        assert t.success is False
        assert t.gas_used == 10
        assert t.value_wei == 0
        assert t.gas_limit == 0
        assert t.nonce == 0
        assert t.calldata_full == ""
        assert t.calldata_selector == ""
        assert t.calldata_decoded == "unknown(empty)"

    def test_bundle_tx_dict_overlays_fields_and_decodes_calldata(self):
        calldata = "0x095ea7b3" + "00" * 32
        bundle_tx = _FakeBundleTx(
            tx_dict={
                "to": "0xTO",
                "value": 42,
                "gas": 1_000_000,
                "nonce": 7,
                "data": calldata,
            }
        )
        bundle = _FakeBundle(transactions=[bundle_tx])
        tr = _FakeTxResult(gas_used=555_000)

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=bundle,
            transaction_results=[tr],
            started_at=STARTED_AT,
        )
        t = report.transactions[0]
        assert t.to_address == "0xTO"
        assert t.value_wei == 42
        assert t.gas_limit == 1_000_000
        assert t.nonce == 7
        assert t.calldata_full == calldata
        assert t.calldata_selector == "0x095ea7b3"
        assert t.calldata_decoded == "approve(address,uint256)"
        assert t.gas_used == 555_000

    def test_receipt_to_address_used_when_bundle_tx_dict_missing(self):
        receipt = _FakeReceipt(to_address="0xFROM-RECEIPT")
        tr = _FakeTxResult(receipt=receipt)
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[tr],
            started_at=STARTED_AT,
        )
        assert report.transactions[0].to_address == "0xFROM-RECEIPT"

    def test_receipt_to_fallback_when_to_address_absent(self):
        receipt = _FakeReceipt(to_address=None, to="0xFROM-TO")
        tr = _FakeTxResult(receipt=receipt)
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[tr],
            started_at=STARTED_AT,
        )
        # Pre-refactor behaviour (pinned): ``getattr(receipt, "to_address", ...)``
        # returns ``None`` outright when the attribute exists and is ``None`` --
        # the fallback to ``receipt.to`` only fires when ``to_address`` is
        # absent entirely. Operator tooling treats ``None`` as "no address
        # on receipt" and falls back to the bundle tx_dict downstream.
        assert report.transactions[0].to_address is None

    def test_receipt_to_used_when_to_address_attribute_missing(self):
        """When ``to_address`` attribute is absent, ``receipt.to`` is used."""

        class _ReceiptNoToAddress:
            to = "0xFROM-TO"

        tr = _FakeTxResult(receipt=_ReceiptNoToAddress())
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[tr],
            started_at=STARTED_AT,
        )
        assert report.transactions[0].to_address == "0xFROM-TO"

    def test_gas_limit_falls_back_to_gaslimit_when_gas_missing(self):
        bundle_tx = _FakeBundleTx(tx_dict={"gasLimit": 321_000})
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(transactions=[bundle_tx]),
            transaction_results=[_FakeTxResult()],
            started_at=STARTED_AT,
        )
        assert report.transactions[0].gas_limit == 321_000

    def test_calldata_falls_back_to_input_when_data_missing(self):
        bundle_tx = _FakeBundleTx(tx_dict={"input": "0xa9059cbb" + "11" * 32})
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(transactions=[bundle_tx]),
            transaction_results=[_FakeTxResult()],
            started_at=STARTED_AT,
        )
        t = report.transactions[0]
        assert t.calldata_selector == "0xa9059cbb"
        assert t.calldata_decoded == "transfer(address,uint256)"

    def test_unknown_selector_decodes_to_unknown_form(self):
        calldata = "0xdeadbeef" + "00" * 32
        bundle_tx = _FakeBundleTx(tx_dict={"data": calldata})
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(transactions=[bundle_tx]),
            transaction_results=[_FakeTxResult()],
            started_at=STARTED_AT,
        )
        assert report.transactions[0].calldata_decoded == "unknown(0xdeadbeef)"

    def test_bundle_tx_without_tx_dict_is_skipped(self):
        """Bundle transactions whose ``tx_dict`` is None/empty are filtered
        out before index alignment. A single real tx_dict at position 0 must
        still apply to transaction_results[0]."""
        bundle = _FakeBundle(
            transactions=[
                _FakeBundleTx(tx_dict=None),
                _FakeBundleTx(tx_dict={"to": "0xSECOND", "value": 9}),
            ]
        )
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=bundle,
            transaction_results=[_FakeTxResult()],
            started_at=STARTED_AT,
        )
        # Only the 2nd bundle tx had a dict, so it becomes bundle_tx_dicts[0]
        # and applies to transaction_results[0].
        assert report.transactions[0].to_address == "0xSECOND"
        assert report.transactions[0].value_wei == 9

    def test_revert_reason_copied_from_tr_error(self):
        tr = _FakeTxResult(error="custom-error")
        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=_FakeBundle(),
            transaction_results=[tr],
            started_at=STARTED_AT,
        )
        assert report.transactions[0].revert_reason == "custom-error"


# ---------------------------------------------------------------------------
# decode_calldata_selector — direct tests
# ---------------------------------------------------------------------------


class TestDecodeCalldataSelector:
    def test_empty_calldata_returns_unknown_empty(self):
        assert decode_calldata_selector("") == "unknown(empty)"

    def test_short_calldata_returns_unknown_empty(self):
        assert decode_calldata_selector("0x12") == "unknown(empty)"

    def test_known_selector_lowercased(self):
        # Uppercase selectors still match because we normalise with lower().
        assert decode_calldata_selector("0x095EA7B3abcd") == "approve(address,uint256)"

    def test_unknown_selector_returns_unknown_form(self):
        assert decode_calldata_selector("0xdeadbeef" + "00" * 32) == "unknown(0xdeadbeef)"


# ---------------------------------------------------------------------------
# VerboseRevertReport.format() — golden-file style assertions
#
# The format() output is scraped by operator tooling and MUST NOT change
# byte-for-byte across this refactor. These tests assert full-string equality
# for representative scenarios.
# ---------------------------------------------------------------------------


MINIMAL_GOLDEN = (
    "\n"
    "======================================================================\n"
    "VERBOSE REVERT REPORT\n"
    "======================================================================\n"
    "\n"
    "--- EXECUTION CONTEXT ---\n"
    "Deployment ID: strat-1\n"
    "Chain: arbitrum\n"
    "Wallet: 0xWALLET\n"
    "Correlation ID: corr-123\n"
    "Intent Description: swap 1 WETH -> USDC\n"
    "\n"
    "Started At: 2026-04-23T10:00:00+00:00\n"
    "Failed At: 2026-04-23T10:00:05+00:00\n"
    "Execution Phase: CONFIRMATION\n"
    "\n"
    "======================================================================"
)


def _build_minimal_report() -> VerboseRevertReport:
    return VerboseRevertReport(
        deployment_id="strat-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
        correlation_id="corr-123",
        intent_description="swap 1 WETH -> USDC",
        started_at=STARTED_AT,
        failed_at=FAILED_AT_FROZEN,
        execution_phase="CONFIRMATION",
    )


class TestFormatMinimal:
    def test_minimal_report_matches_golden(self):
        report = _build_minimal_report()
        assert report.format() == MINIMAL_GOLDEN


class TestFormatIntentBlock:
    def test_intent_block_rendered_with_params(self):
        report = _build_minimal_report()
        report.intent = IntentDetails(
            intent_type="SWAP",
            intent_id="intent-1",
            params={"from_token": "WETH", "to_token": "USDC"},
        )
        text = report.format()
        assert "--- INTENT DETAILS ---" in text
        assert "  Intent Type: SWAP" in text
        assert "  Intent ID: intent-1" in text
        assert "  Parameters:" in text
        assert "    from_token: WETH" in text
        assert "    to_token: USDC" in text

    def test_intent_block_skips_private_keys(self):
        report = _build_minimal_report()
        report.intent = IntentDetails(
            intent_type="SWAP",
            intent_id="intent-1",
            params={"visible": "yes", "_hidden": "no"},
        )
        text = report.format()
        assert "visible: yes" in text
        assert "_hidden" not in text

    def test_intent_long_value_truncated_to_80_plus_ellipsis(self):
        long_val = "x" * 200
        report = _build_minimal_report()
        report.intent = IntentDetails(
            intent_type="SWAP",
            intent_id="id",
            params={"big": long_val},
        )
        text = report.format()
        assert f"    big: {'x' * 80}..." in text


class TestFormatActionsBlock:
    def test_actions_block_numbering_and_total(self):
        report = _build_minimal_report()
        report.actions = [
            ActionDetails(action_type="APPROVE", protocol="erc20", params={"a": 1}),
            ActionDetails(action_type="SWAP", protocol="uniswap_v3", params={"b": 2}),
        ]
        text = report.format()
        assert "--- ACTIONS ---" in text
        assert "  Action 1/2:" in text
        assert "  Action 2/2:" in text
        assert "    Type: APPROVE" in text
        assert "    Protocol: erc20" in text
        assert "    Type: SWAP" in text
        assert "    Protocol: uniswap_v3" in text

    def test_actions_long_param_value_truncated(self):
        long_val = "y" * 200
        report = _build_minimal_report()
        report.actions = [ActionDetails(action_type="SWAP", protocol="uniswap_v3", params={"long": long_val})]
        text = report.format()
        assert f"      long: {'y' * 80}..." in text


class TestFormatTransactionsBlock:
    def test_transactions_block_success_rendering(self):
        report = _build_minimal_report()
        report.transactions = [
            TransactionDetails(
                tx_hash="0xaaa",
                to_address="0xTO",
                value_wei=100,
                gas_limit=21000,
                gas_used=20500,
                nonce=3,
                calldata_selector="0x095ea7b3",
                calldata_decoded="approve(address,uint256)",
                calldata_full="0x095ea7b3" + "00" * 32,
                success=True,
                revert_reason=None,
            )
        ]
        text = report.format()
        assert "--- TRANSACTIONS ---" in text
        assert "  Transaction 1/1:" in text
        assert "    TX Hash: 0xaaa" in text
        assert "    Status: SUCCESS" in text
        assert "    To: 0xTO" in text
        assert "    Value: 100 wei" in text
        assert "    Gas Limit: 21,000" in text
        assert "    Gas Used: 20,500" in text
        assert "    Nonce: 3" in text
        assert "    Function: approve(address,uint256)" in text
        assert "    Calldata: 0x095ea7b3..." in text

    def test_transaction_reverted_and_revert_reason_rendered(self):
        report = _build_minimal_report()
        report.transactions = [
            TransactionDetails(
                tx_hash="0xbbb",
                to_address="0xTO",
                value_wei=0,
                gas_limit=100_000,
                gas_used=None,
                nonce=1,
                calldata_selector="0xdeadbeef",
                calldata_decoded="unknown(0xdeadbeef)",
                calldata_full="0xdeadbeef",
                success=False,
                revert_reason="STF",
            )
        ]
        text = report.format()
        assert "    Status: REVERTED" in text
        assert "    Gas Used: N/A" in text
        assert "    Revert Reason: STF" in text


class TestFormatRawError:
    def test_raw_error_absent_omits_section(self):
        report = _build_minimal_report()
        text = report.format()
        assert "--- RAW ERROR ---" not in text

    def test_raw_error_short_is_preserved(self):
        report = _build_minimal_report()
        report.raw_error = "short error"
        text = report.format()
        assert "--- RAW ERROR ---\nshort error\n" in text

    def test_raw_error_long_is_truncated_to_500_chars(self):
        report = _build_minimal_report()
        report.raw_error = "e" * 700
        text = report.format()
        # 500 'e' chars followed by blank line then closing separator.
        assert "--- RAW ERROR ---\n" + ("e" * 500) + "\n" in text
        assert ("e" * 501) not in text


# ---------------------------------------------------------------------------
# Integration — build + format producing a stable large-scale golden.
# ---------------------------------------------------------------------------


class TestBuildAndFormatIntegration:
    def test_full_pipeline_byte_for_byte(self, monkeypatch):
        _freeze_failed_at(monkeypatch)

        class _Params:
            def model_dump(self) -> dict[str, Any]:
                return {"from_token": "WETH", "to_token": "USDC"}

        class _Intent:
            intent_type = _EnumLike("SWAP")
            id = "intent-xyz"

            def model_dump(self) -> dict[str, Any]:
                return {"from_token": "WETH", "to_token": "USDC", "amount": "1.5"}

        action = _FakeAction(
            type=_EnumLike("SWAP"),
            protocol=_EnumLike("uniswap_v3"),
            params=_Params(),
        )
        bundle_tx = _FakeBundleTx(
            tx_dict={
                "to": "0xROUTER",
                "value": 0,
                "gas": 500_000,
                "nonce": 2,
                "data": "0x414bf389" + "00" * 32,
            }
        )
        bundle = _FakeBundle(actions=[action], transactions=[bundle_tx])
        tr = _FakeTxResult(
            tx_hash="0xTX",
            success=False,
            gas_used=400_000,
            error="Too little received",
        )

        report = build_verbose_revert_report(
            context=_FakeContext(),
            action_bundle=bundle,
            transaction_results=[tr],
            intent=_Intent(),
            raw_error="Too little received",
            started_at=STARTED_AT,
        )

        text = report.format()
        # Spot-check each phase appears in the expected order.
        idx_context = text.index("--- EXECUTION CONTEXT ---")
        idx_intent = text.index("--- INTENT DETAILS ---")
        idx_actions = text.index("--- ACTIONS ---")
        idx_tx = text.index("--- TRANSACTIONS ---")
        idx_raw = text.index("--- RAW ERROR ---")
        assert idx_context < idx_intent < idx_actions < idx_tx < idx_raw

        # Specific render lines.
        assert "    Function: exactInputSingle(ExactInputSingleParams)" in text
        assert "    Status: REVERTED" in text
        assert "    Revert Reason: Too little received" in text
        assert "Too little received" in text  # raw error section

    def test_to_dict_round_trip_stable(self):
        report = _build_minimal_report()
        report.intent = IntentDetails(intent_type="SWAP", intent_id="i", params={"k": "v"})
        report.actions = [ActionDetails(action_type="SWAP", protocol="p", params={})]
        report.transactions = [
            TransactionDetails(
                tx_hash="h",
                to_address="to",
                value_wei=0,
                gas_limit=0,
                gas_used=None,
                nonce=0,
                calldata_selector="",
                calldata_decoded="unknown(empty)",
                calldata_full="",
                success=True,
                revert_reason=None,
            )
        ]
        d = report.to_dict()
        assert d["deployment_id"] == "strat-1"
        assert d["intent"]["intent_type"] == "SWAP"
        assert d["actions"][0]["action_type"] == "SWAP"
        assert d["transactions"][0]["tx_hash"] == "h"
        assert d["started_at"] == STARTED_AT.isoformat()
        assert d["failed_at"] == FAILED_AT_FROZEN.isoformat()
