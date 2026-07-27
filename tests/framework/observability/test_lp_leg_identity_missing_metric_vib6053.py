"""VIB-6053 residual-surface observability: ``ledger_lp_leg_identity_missing_total``.

Follow-up over PR #3451 (bind LP leg identity at the parser). #3451 makes the parser
stamp ``currency0``/``currency1`` (or ``coin_symbols``) so the ledger binds LP symbols by
on-chain ADDRESS; a leg with no identity still falls back to the intent/pool LABEL order
(the residual phantom-order surface). This metric makes that fallback VISIBLE — it is
observability ONLY and does not change the row — so the parser-stamp rollout can be driven
to completion, mirroring the ``ledger_intent_fallback_total`` contract (VIB-5218).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.ledger import build_ledger_entry
from almanak.framework.observability.metrics import LEDGER_LP_LEG_IDENTITY_MISSING_TOTAL


class _NoIdentityIntent:
    token0 = "WETH"
    token1 = "USDC"
    pool = "WETH/USDC/500"

    def __init__(self, intent_type: str, protocol: str) -> None:
        self.intent_type = intent_type
        self.protocol = protocol


class _Result:
    swap_amounts = None
    success = True
    tx_hash = "0xnoident"
    total_gas_cost_wei = 0

    def __init__(self, extracted: dict[str, Any]) -> None:
        self.extracted_data = extracted

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return None


def _count(protocol: str, intent_type: str) -> float:
    return LEDGER_LP_LEG_IDENTITY_MISSING_TOTAL.labels(
        protocol=protocol, chain="ethereum", intent_type=intent_type
    )._value.get()


@pytest.mark.parametrize(
    "intent_type,data",
    [
        ("LP_CLOSE", LPCloseData(amount0_collected=1_000_000, amount1_collected=2_000_000, currency0=None, currency1=None)),
        ("LP_OPEN", LPOpenData(position_id=1, amount0=1_000_000, amount1=2_000_000, currency0=None, currency1=None)),
    ],
)
def test_no_identity_lp_row_increments_metric_and_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, intent_type: str, data: Any
) -> None:
    proto = "some_unstamped_dex"
    key = "lp_close_data" if intent_type == "LP_CLOSE" else "lp_open_data"
    before = _count(proto, intent_type)
    with caplog.at_level(logging.WARNING):
        entry = build_ledger_entry(
            deployment_id="d", cycle_id="c", intent=_NoIdentityIntent(intent_type, proto), result=_Result({key: data}), chain="ethereum", success=True
        )
    assert entry is not None
    # Observability ONLY — the row is still written (label order), unchanged.
    assert entry.token_in == "WETH"
    assert _count(proto, intent_type) == before + 1, "identity-missing must be metered"
    assert any("intent-LABEL order" in r.getMessage() for r in caplog.records), "must WARN"


def test_stamped_identity_does_not_increment(monkeypatch: pytest.MonkeyPatch) -> None:
    """A row WITH currency identity must NOT count as identity-missing."""
    proto = "uniswap_v4"
    before = _count(proto, "LP_CLOSE")
    data = LPCloseData(
        amount0_collected=1_000_000, amount1_collected=2_000_000, currency0="0x" + "11" * 20, currency1="0x" + "cc" * 20
    )
    entry = build_ledger_entry(
        deployment_id="d", cycle_id="c", intent=_NoIdentityIntent("LP_CLOSE", proto), result=_Result({"lp_close_data": data}), chain="ethereum", success=True
    )
    assert entry is not None
    assert _count(proto, "LP_CLOSE") == before, "stamped-identity row must not be metered"
