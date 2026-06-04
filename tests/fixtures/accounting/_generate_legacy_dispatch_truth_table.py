"""Generate the FROZEN PRE-T3 dispatch truth table (VIB-4163).

Runs the LEGACY if-ladder dispatcher in
``almanak/framework/accounting/processor.py:_dispatch`` against a curated synthetic
input matrix and captures the resulting event's class name, event_type field, and
payload_json. The output is committed at
``tests/fixtures/accounting/legacy_dispatch_truth_table.json`` in the precursor
commit so the post-T3 parity test (D1.S2 in the UAT card) has a frozen reference.

Run from the repo root with the LEGACY processor.py (i.e. before the T3 commit):

    uv run python tests/fixtures/accounting/_generate_legacy_dispatch_truth_table.py

The generator deliberately uses minimal synthetic inputs (fixed timestamp, empty
strings for most fields) so the output is deterministic. The handlers' lazy
fallback for missing fields (None, "UNKNOWN", UNAVAILABLE confidence) is well-defined
and will reproduce identically across runs.

This file is committed for auditability — any reviewer can re-run it and see the
same JSON. It is NOT part of the test suite (no ``test_*`` prefix) and is not
imported by any module.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.processor import AccountingProcessor
from almanak.framework.primitives.types import AccountingCategory

# Fixed timestamp + identity inputs so handler outputs are byte-deterministic.
_FIXED_TS = "2026-01-01T00:00:00+00:00"
_FIXED_DEPLOYMENT_ID = "AccountingTest:vib4163-fixture"
_FIXED_DEPLOYMENT_ID = "vib4163-fixture"
_FIXED_CYCLE_ID = "cycle-1"
_FIXED_TX_HASH = "0xfeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface"
_FIXED_WALLET = "0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd"
_FIXED_LEDGER_ENTRY_ID = "led-vib4163-fixture"


def _base_outbox(*, position_key: str = "", market_id: str = "") -> dict[str, Any]:
    return {
        "id": "ob-vib4163-fixture",
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "cycle_id": _FIXED_CYCLE_ID,
        "ledger_entry_id": _FIXED_LEDGER_ENTRY_ID,
        "wallet_address": _FIXED_WALLET,
        "position_key": position_key,
        "market_id": market_id,
        "intent_type": "",
    }


def _base_ledger(
    *,
    intent_type: str,
    protocol: str = "",
    token_in: str = "",
    token_out: str = "",
) -> dict[str, Any]:
    return {
        "id": _FIXED_LEDGER_ENTRY_ID,
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "deployment_id": _FIXED_DEPLOYMENT_ID,
        "cycle_id": _FIXED_CYCLE_ID,
        "execution_mode": "live",
        "chain": "arbitrum",
        "protocol": protocol,
        "tx_hash": _FIXED_TX_HASH,
        "timestamp": _FIXED_TS,
        "intent_type": intent_type,
        "token_in": token_in,
        "token_out": token_out,
        "amount_in": "",
        "amount_out": "",
        "effective_price": None,
        "slippage_bps": None,
        "gas_usd": None,
        "extracted_data_json": "",
        "price_inputs_json": "",
        "post_state_json": "",
        "pre_state_json": "",
    }


_PRIOR_OPEN_PAYLOAD: dict[str, Any] = {
    "event_type": "LP_OPEN",
    "position_key": "lp:arbitrum:0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd:pool",
    "asset0": "USDC",
    "asset1": "WETH",
    "tick_lower": -887220,
    "tick_upper": 887220,
    "schema_version": 1,
}


def _build_processor() -> AccountingProcessor:
    """Construct an AccountingProcessor with a stub state_manager + in-memory basis store."""

    class _StubStateManager:
        def get_accounting_events_sync(
            self, deployment_id: str, position_key: str = ""
        ) -> list[dict[str, Any]]:
            # Return a single prior LP_OPEN row so handle_lp can find it on LP_CLOSE.
            if position_key:
                return [{"event_type": "LP_OPEN", "payload_json": json.dumps(_PRIOR_OPEN_PAYLOAD)}]
            return []

    basis_store = FIFOBasisStore()
    return AccountingProcessor(
        state_manager=_StubStateManager(),
        basis_store=basis_store,
        deployment_id=_FIXED_DEPLOYMENT_ID,
    )


# (category, label, outbox, ledger, expected_class)
_FIXTURE_MATRIX: list[tuple[AccountingCategory, str, dict[str, Any], dict[str, Any], str]] = [
    (
        AccountingCategory.LENDING,
        "supply",
        _base_outbox(position_key="lending:aave_v3:arbitrum:USDC"),
        _base_ledger(intent_type="SUPPLY", protocol="aave_v3", token_in="USDC"),
        "LendingAccountingEvent",
    ),
    (
        AccountingCategory.LP,
        "open",
        _base_outbox(position_key="lp:arbitrum:0xabcd…:0x1111111111111111111111111111111111111111"),
        _base_ledger(intent_type="LP_OPEN", protocol="uniswap_v3", token_in="USDC", token_out="WETH"),
        "LPAccountingEvent",
    ),
    (
        AccountingCategory.LP,
        "close",
        _base_outbox(position_key="lp:arbitrum:0xabcd…:0x1111111111111111111111111111111111111111"),
        _base_ledger(intent_type="LP_CLOSE", protocol="uniswap_v3", token_in="USDC", token_out="WETH"),
        "LPAccountingEvent",
    ),
    (
        # VIB-4931: Pendle LP now resolves to the generic LP category; the connector
        # treatment (registry stage-1) still produces the PendleAccountingEvent.
        AccountingCategory.LP,
        "pendle_open",
        _base_outbox(position_key="pendle_lp:arbitrum:WETH-PT"),
        _base_ledger(intent_type="LP_OPEN", protocol="pendle_v2", token_in="WETH", token_out="PT-WETH"),
        "PendleAccountingEvent",
    ),
    (
        # VIB-4931: Pendle PT now resolves to the generic SWAP category.
        AccountingCategory.SWAP,
        "pendle_buy",
        _base_outbox(position_key="pendle_pt:arbitrum:PT-WETH"),
        _base_ledger(intent_type="SWAP", protocol="pendle_v2", token_in="WETH", token_out="PT-WETH"),
        "PendleAccountingEvent",
    ),
    (
        AccountingCategory.PERP,
        "open",
        _base_outbox(position_key="perp:gmx_v2:arbitrum:WETH"),
        _base_ledger(intent_type="PERP_OPEN", protocol="gmx_v2", token_in="USDC"),
        "PerpAccountingEvent",
    ),
    (
        AccountingCategory.VAULT,
        "deposit",
        _base_outbox(position_key="vault:morpho:arbitrum:USDC"),
        _base_ledger(intent_type="VAULT_DEPOSIT", protocol="morpho_blue", token_in="USDC"),
        "VaultAccountingEvent",
    ),
    (
        AccountingCategory.SWAP,
        "swap",
        _base_outbox(position_key="swap:arbitrum:0xabcd…"),
        _base_ledger(intent_type="SWAP", protocol="enso", token_in="USDC", token_out="WETH"),
        "SwapAccountingEvent",
    ),
    (
        AccountingCategory.PREDICTION,
        "buy",
        _base_outbox(position_key="prediction:polymarket:0xmarket:YES"),
        _base_ledger(intent_type="PREDICTION_BUY", protocol="polymarket", token_in="USDC"),
        "PredictionAccountingEvent",
    ),
]


def _normalize_payload(raw: str) -> dict[str, Any]:
    """Decode the JSON payload to a dict for stable cross-run equality.

    Re-encoding via ``json.dumps(..., sort_keys=True)`` would let us byte-compare,
    but storing the dict form makes the fixture human-readable and lets the parity
    test load it once and compare structurally.
    """
    return json.loads(raw)


def build_truth_table_json_text() -> str:
    """Return the canonical truth-table JSON text the generator would write.

    Pure function — used by ``main()`` to write the file AND by the
    hand-edit-detection test (``test_truth_table_matches_generator_output``)
    to verify the committed file is byte-identical to what the generator
    would produce today. Catches hand-edits without hard-coding a single
    permitted commit (which would block legitimate additive payload-shape
    changes — VIB-4166 added ``primitive_version`` to every event's
    ``to_payload_json`` output, which the legacy dispatcher's captured
    payloads must mirror, regenerated through this same function).
    """
    processor = _build_processor()
    rows = []
    for category, label, outbox, ledger, expected_class in _FIXTURE_MATRIX:
        event = processor._dispatch(outbox, ledger)
        if event is None:
            row: dict[str, Any] = {
                "category": category.value,
                "label": label,
                "intent_type": ledger["intent_type"],
                "protocol": ledger["protocol"],
                "expected_event_class": None,
                "expected_event_type": None,
                "expected_payload": None,
            }
        else:
            row = {
                "category": category.value,
                "label": label,
                "intent_type": ledger["intent_type"],
                "protocol": ledger["protocol"],
                "expected_event_class": type(event).__name__,
                "expected_event_type": getattr(event.event_type, "value", str(event.event_type)),
                "expected_payload": _normalize_payload(event.to_payload_json()),
            }
            assert row["expected_event_class"] == expected_class, (
                f"unexpected event class for {category.value}/{label}: "
                f"got {row['expected_event_class']}, declared {expected_class}"
            )
        rows.append(row)

    out = {
        "schema_version": 1,
        "ticket": "VIB-4163",
        "description": (
            "Frozen pre-T3 dispatch truth table. Generated by "
            "_generate_legacy_dispatch_truth_table.py against the LEGACY if-ladder "
            "dispatcher in processor.py. Used by the post-T3 parity test (D1.S2). "
            "DO NOT edit by hand — re-run the generator script if the legacy "
            "dispatcher changes (and regenerate before T3 ships)."
        ),
        "fixtures": rows,
    }
    return json.dumps(out, indent=2, sort_keys=False) + "\n"


def main() -> None:
    text = build_truth_table_json_text()
    target = Path(__file__).parent / "legacy_dispatch_truth_table.json"
    target.write_text(text)
    fixture_count = json.loads(text)["fixtures"]
    print(f"wrote {target} ({len(fixture_count)} fixtures)")


if __name__ == "__main__":
    main()
