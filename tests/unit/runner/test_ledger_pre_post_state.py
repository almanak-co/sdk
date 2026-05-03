"""Tests for the pre_state / post_state ledger wiring
(Accounting-AttemptNo17 §A4 — VIB-3480 columns finally populated).

Until this landed, ``transaction_ledger.pre_state_json`` and
``post_state_json`` were NULL on every ledger row even though the
runner had pre/post wallet balance observations in scope.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.runner.strategy_runner import (
    _build_post_state_for_ledger,
    _build_pre_state_for_ledger,
)


def _balance_snapshot(balances: dict[str, Decimal] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        balances=balances or {},
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
    )


def test_pre_state_returns_none_when_snapshot_missing():
    assert _build_pre_state_for_ledger(None) is None


def test_pre_state_returns_none_when_balances_empty():
    assert _build_pre_state_for_ledger(_balance_snapshot({})) is None


def test_pre_state_serialises_balances_as_strings():
    snap = _balance_snapshot({"USDC": Decimal("19.0"), "WETH": Decimal("0.001234")})
    out = _build_pre_state_for_ledger(snap)
    assert out is not None
    assert out["wallet_balances"] == {"USDC": "19.0", "WETH": "0.001234"}
    assert out["captured_at"] == "2026-05-01T12:00:00+00:00"
    assert out["source"] == "balance_provider"


def test_post_state_returns_none_when_recon_missing():
    assert _build_post_state_for_ledger(None) is None


def test_post_state_returns_none_when_no_post_balances():
    recon = {"tokens_checked": ["USDC"], "warnings": []}  # no post_balances
    assert _build_post_state_for_ledger(recon) is None


def test_post_state_pulls_from_recon():
    recon = {
        "tokens_checked": ["USDC", "WETH"],
        "post_balances": {"USDC": "20.5", "WETH": "0.001"},
        "post_timestamp": "2026-05-01T12:00:30+00:00",
        "incident": False,
    }
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert out["wallet_balances"] == {"USDC": "20.5", "WETH": "0.001"}
    assert out["captured_at"] == "2026-05-01T12:00:30+00:00"
    assert out["source"] == "balance_provider"
    assert out["incident"] is False


def test_post_state_threads_incident_flag_for_recon_failure_path():
    # VIB-3480 use case: an incident row should still carry post-state so
    # auditors can see the on-chain state at the time of the breach.
    recon = {
        "post_balances": {"USDC": "0"},
        "incident": True,
        "mismatches": ["something"],
    }
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert out["incident"] is True


def test_post_state_handles_missing_post_timestamp():
    """VIB-3888: legacy recon without ``post_timestamp`` now stamps
    ``datetime.now(UTC)`` rather than the empty-string the pre-VIB-3888
    builder emitted. The reconciliation's existence implies the
    post-balance read just ran a few ms earlier; an immediate-now stamp
    is a closer approximation than NULL, which the Accountant Test G6
    per-intent path needs to exist in some form."""
    recon = {"post_balances": {"USDC": "20.5"}}
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    # VIB-3888 — captured_at is non-empty and parseable as ISO-8601 even
    # when the legacy recon shape didn't propagate ``post_timestamp``.
    from datetime import datetime as _dt

    assert out["captured_at"]
    _dt.fromisoformat(out["captured_at"])  # raises if not parseable


# ──────────────────────────────────────────────────────────────────────────────
# VIB-3474 — lending protocol state merge into pre_state / post_state
# ──────────────────────────────────────────────────────────────────────────────


def _aave_state(collateral_usd: str, debt_usd: str, hf: str, lt_bps: int):
    from almanak.framework.accounting.lending_accounting import AaveAccountState

    return AaveAccountState(
        collateral_usd=Decimal(collateral_usd),
        debt_usd=Decimal(debt_usd),
        health_factor=Decimal(hf),
        liquidation_threshold_bps=lt_bps,
    )


def _morpho_state(collateral_usd: str, debt_usd: str, hf: str, lltv: str):
    from almanak.framework.accounting.lending_accounting import MorphoBlueAccountState

    return MorphoBlueAccountState(
        collateral_usd=Decimal(collateral_usd),
        debt_usd=Decimal(debt_usd),
        health_factor=Decimal(hf),
        lltv=Decimal(lltv),
    )


def test_pre_state_merges_aave_lending_state_with_balances():
    snap = _balance_snapshot({"USDC": Decimal("19.0")})
    aave = _aave_state("15420.50", "8200.00", "1.882", 8500)
    out = _build_pre_state_for_ledger(snap, aave, protocol="aave_v3")
    assert out is not None
    assert out["wallet_balances"] == {"USDC": "19.0"}
    assert out["protocol"] == "aave_v3"
    assert out["collateral_usd"] == "15420.50"
    assert out["debt_usd"] == "8200.00"
    assert out["health_factor"] == "1.882"
    assert out["liquidation_threshold_bps"] == 8500
    # Source is layered so audits can see both capture origins.
    assert "lending_capture" in out["source"]


def test_pre_state_lending_only_when_balances_empty():
    """VIB-3474: lending state alone is sufficient — wallet balances optional."""
    aave = _aave_state("100.0", "50.0", "2.0", 8000)
    out = _build_pre_state_for_ledger(None, aave, protocol="aave_v3")
    assert out is not None
    assert "wallet_balances" not in out
    assert out["collateral_usd"] == "100.0"
    assert out["protocol"] == "aave_v3"
    assert out["liquidation_threshold_bps"] == 8000


def test_post_state_merges_morpho_lending_state():
    """VIB-3474: Morpho Blue surfaces lltv AND a derived liquidation_threshold_bps."""
    recon = {"post_balances": {"USDC": "20.5"}, "post_timestamp": ""}
    morpho = _morpho_state("9000.0", "4000.0", "1.935", "0.86")
    out = _build_post_state_for_ledger(recon, morpho, protocol="morpho_blue")
    assert out is not None
    assert out["wallet_balances"] == {"USDC": "20.5"}
    assert out["protocol"] == "morpho_blue"
    assert out["lltv"] == "0.86"
    assert out["liquidation_threshold_bps"] == 8600  # 0.86 × 10000 = 8600


def test_pre_state_returns_none_when_no_balances_and_no_lending():
    assert _build_pre_state_for_ledger(None, None) is None
    assert _build_pre_state_for_ledger(_balance_snapshot({}), None) is None


def test_lending_state_to_dict_returns_none_for_none_state():
    """Honest absence over fabricated zeros — VIB-3474 hard rule."""
    from almanak.framework.accounting.lending_accounting import lending_state_to_dict

    assert lending_state_to_dict(None, protocol="aave_v3") is None


def test_capture_lending_state_safe_returns_none_without_gateway():
    """No gateway client → no capture (local without gateway / paper)."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    intent = SimpleNamespace(intent_type="SUPPLY", protocol="aave_v3")
    out = StrategyRunner._capture_lending_state_safe(
        intent=intent,
        chain="ethereum",
        wallet_address="0x" + "a" * 40,
        gateway_client=None,
        price_oracle={},
        phase="pre",
    )
    assert out is None


def test_capture_lending_state_safe_skips_non_lending_intents():
    """SWAP / LP_OPEN / PERP_OPEN must not trigger lending state reads."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def eth_call(self, *_, **__):
            self.calls += 1
            return "0x" + "00" * 32 * 6

    gateway = _GatewayProbe()
    for intent_type in ("SWAP", "LP_OPEN", "LP_CLOSE", "PERP_OPEN", "PERP_CLOSE"):
        intent = SimpleNamespace(intent_type=intent_type, protocol="aave_v3")
        out = StrategyRunner._capture_lending_state_safe(
            intent=intent,
            chain="ethereum",
            wallet_address="0x" + "a" * 40,
            gateway_client=gateway,
            price_oracle={},
            phase="pre",
        )
        assert out is None
    assert gateway.calls == 0  # never invoked the gateway


def test_capture_lending_state_safe_aave_calls_gateway():
    """SUPPLY on Aave V3 with a working gateway returns AaveAccountState.

    Anti-regression contract for VIB-3474 / iter-176 VIB-2986 shape: the
    gateway-backed flow MUST exercise the populate path. The previous bug
    let the legacy validation skip silently when ``rpc_url=None`` was passed
    alongside a gateway_client. This test asserts the ``capture_lending_*``
    helpers still reach the gateway when the runner threads it through.
    """
    from almanak.framework.accounting.lending_accounting import AaveAccountState
    from almanak.framework.runner.strategy_runner import StrategyRunner

    # Build a synthetic getUserAccountData response: 6 words, 32 bytes each.
    # Word order: collateralBase, debtBase, _availableBorrows, ltBps, _ltv, hf.
    # collateral=15420.50e8, debt=8200.00e8, lt=8500 bps, hf=1.882e18
    def _hex(n: int) -> str:
        return f"{n:064x}"

    words = [
        _hex(int(Decimal("15420.50") * Decimal("1e8"))),
        _hex(int(Decimal("8200.00") * Decimal("1e8"))),
        _hex(0),
        _hex(8500),
        _hex(0),
        _hex(int(Decimal("1.882") * Decimal("1e18"))),
    ]
    aave_response = "0x" + "".join(words)

    class _Gateway:
        def __init__(self):
            self.calls: list[tuple[str, str, str]] = []

        def eth_call(self, chain: str, to: str, data: str) -> str:
            self.calls.append((chain, to, data))
            return aave_response

    gateway = _Gateway()
    intent = SimpleNamespace(intent_type="SUPPLY", protocol="aave_v3", token="USDC", market_id="")
    out = StrategyRunner._capture_lending_state_safe(
        intent=intent,
        chain="ethereum",
        wallet_address="0x" + "a" * 40,
        gateway_client=gateway,
        price_oracle={"USDC": "1.0"},
        phase="pre",
    )
    assert isinstance(out, AaveAccountState)
    assert out.collateral_usd == Decimal("15420.50")
    assert out.debt_usd == Decimal("8200.00")
    assert out.liquidation_threshold_bps == 8500
    assert out.health_factor == Decimal("1.882")
    # The gateway WAS called — the populate path is exercised, not silently skipped.
    assert len(gateway.calls) == 1


def test_lending_handler_reads_runner_serialized_post_state():
    """End-to-end: AaveAccountState → lending_state_to_dict → JSON →
    handle_lending sees collateral_usd / debt_usd / health_factor populated.

    This is the anti-regression contract: when the runner captures lending
    state and ships it into post_state_json, the AccountingProcessor lending
    handler reads back HIGH-confidence fields — no live chain calls, no
    ESTIMATED fallback. Mirrors the iter-176 VIB-2986 anti-regression shape:
    the gateway-backed flow must populate the column.
    """
    import json
    import uuid

    from almanak.framework.accounting.basis import FIFOBasisStore
    from almanak.framework.accounting.category_handlers.lending_handler import handle_lending
    from almanak.framework.accounting.lending_accounting import lending_state_to_dict
    from almanak.framework.accounting.models import AccountingConfidence

    aave = _aave_state("15420.50", "8200.00", "1.882", 8500)
    post_state_dict = lending_state_to_dict(aave, protocol="aave_v3")
    assert post_state_dict is not None
    post_state_json = json.dumps(post_state_dict)

    led_id = str(uuid.uuid4())
    extracted = json.dumps({"supply_amount": 100_000_000})
    price_inputs = json.dumps({"USDC": {"price_usd": "1.0", "oracle_source": "x"}})
    outbox = {
        "ledger_entry_id": led_id,
        "intent_type": "SUPPLY",
        "deployment_id": "d",
        "strategy_id": "s",
        "cycle_id": "c",
        "wallet_address": "0x" + "a" * 40,
        "position_key": "lending:ethereum:aave_v3:0x" + "a" * 40 + ":USDC",
        "market_id": "",
    }
    ledger = {
        "id": led_id,
        "intent_type": "SUPPLY",
        "deployment_id": "d",
        "strategy_id": "s",
        "cycle_id": "c",
        "execution_mode": "live",
        "chain": "ethereum",
        "protocol": "aave_v3",
        "tx_hash": "0x" + "b" * 64,
        "token_in": "USDC",
        "extracted_data_json": extracted,
        "price_inputs_json": price_inputs,
        "post_state_json": post_state_json,
        "pre_state_json": "",
        "gas_usd": "0",
        "timestamp": "2026-05-02T12:00:00+00:00",
    }

    from unittest.mock import patch

    class _MockToken:
        decimals = 6

    class _MockResolver:
        def resolve(self, *_, **__):
            return _MockToken()

    with patch(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        return_value=_MockResolver(),
    ):
        event = handle_lending(outbox, ledger, FIFOBasisStore())

    assert event is not None
    # The gateway-backed flow now populates HIGH confidence (VIB-3474).
    assert event.confidence == AccountingConfidence.HIGH
    assert event.collateral_value_after_usd == Decimal("15420.50")
    assert event.debt_value_after_usd == Decimal("8200.00")
    assert event.health_factor_after == Decimal("1.882")
    assert event.liquidation_threshold == Decimal("8500") / Decimal("10000")
    assert event.unavailable_reason == ""
