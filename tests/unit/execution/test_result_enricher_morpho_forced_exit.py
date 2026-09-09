"""Production enrichment path for a Morpho Vault V2 forced exit (VAULT_REDEEM).

Drives ``ResultEnricher._extract_field`` with the REAL ``MetaMorphoReceiptParser``
over the two receipt arrangements a forced exit produces:

* EOA route — the ``forceDeallocate`` tx (penalty Withdraw, receiver == vault)
  is a separate receipt that precedes the redeem tx;
* Safe route — both Withdraws land in one multisend receipt.

Before this fix the first arrangement reported the 0.01 USDC penalty as the
redemption (first successful per-receipt extraction wins) and strategies read
it through ``on_intent_executed`` as their payout.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors.morpho_vault import MetaMorphoReceiptParser
from almanak.connectors.morpho_vault.receipt_parser import EVENT_TOPICS
from almanak.framework.execution.result_enricher import ResultEnricher

VAULT = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"
OWNER = "0x" + "33" * 20
PAYOUT_ASSETS = 99_990_000
PENALTY_ASSETS = 10_000
PAYOUT_SHARES = 96 * 10**18
PENALTY_SHARES = 10**13


def _withdraw_log(receiver: str, assets: int, shares: int) -> dict[str, Any]:
    return {
        "address": VAULT,
        "topics": [
            EVENT_TOPICS["Withdraw"],
            "0x" + "0" * 24 + OWNER[2:],
            "0x" + "0" * 24 + receiver[2:],
            "0x" + "0" * 24 + OWNER[2:],
        ],
        "data": "0x" + hex(assets)[2:].zfill(64) + hex(shares)[2:].zfill(64),
    }


def _receipt(logs: list[dict[str, Any]], tx: str) -> dict[str, Any]:
    return {"transactionHash": tx, "blockNumber": 1, "status": 1, "logs": logs}


PENALTY_RECEIPT = _receipt([_withdraw_log(VAULT, PENALTY_ASSETS, PENALTY_SHARES)], "0x" + "f0" * 32)
REDEEM_RECEIPT = _receipt([_withdraw_log(OWNER, PAYOUT_ASSETS, PAYOUT_SHARES)], "0x" + "f1" * 32)
COMBINED_RECEIPT = _receipt(
    [_withdraw_log(VAULT, PENALTY_ASSETS, PENALTY_SHARES), _withdraw_log(OWNER, PAYOUT_ASSETS, PAYOUT_SHARES)],
    "0x" + "f2" * 32,
)


@dataclass
class _Result:
    extracted_data: dict = field(default_factory=dict)
    redeem_data: Any = None
    protocol_fees: Any = None
    extraction_warnings: list = field(default_factory=list)


def _enrich(receipts: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Run the production extraction and return what the enricher recorded for ``redeem_data``."""
    result = _Result()
    ResultEnricher(live_mode=False)._extract_field(
        result=result,  # type: ignore[arg-type]
        parser=MetaMorphoReceiptParser(),
        receipts=receipts,
        field="redeem_data",
        intent_type="VAULT_REDEEM",
        protocol="metamorpho",
    )
    return result.extracted_data.get("redeem_data")


def test_ordinary_redeem_control() -> None:
    data = _enrich([REDEEM_RECEIPT])
    assert data is not None
    assert data["assets_received"] == PAYOUT_ASSETS
    assert data["penalty_assets"] == 0


def test_eoa_route_penalty_receipt_then_redeem_receipt_reports_the_payout() -> None:
    data = _enrich([PENALTY_RECEIPT, REDEEM_RECEIPT])
    assert data is not None, "redeem data missing"
    assert data["assets_received"] == PAYOUT_ASSETS
    assert data["shares_burned"] == PAYOUT_SHARES
    assert data["penalty_assets"] == PENALTY_ASSETS
    assert data["penalty_shares"] == PENALTY_SHARES


def test_safe_route_single_receipt_reports_the_payout() -> None:
    data = _enrich([COMBINED_RECEIPT])
    assert data is not None
    assert data["assets_received"] == PAYOUT_ASSETS
    assert data["penalty_assets"] == PENALTY_ASSETS


def test_penalty_only_bundle_reports_no_redemption() -> None:
    """A forced exit whose trailing redeem never landed must not claim a payout."""
    assert _enrich([PENALTY_RECEIPT]) is None
