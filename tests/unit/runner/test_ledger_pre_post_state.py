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


def test_post_state_persists_block_anchoring_provenance():
    """VIB-3350: the block-anchoring flags reach post_state_json so the audit
    trail can prove a reconciliation was pinned to the receipt block."""
    recon = {
        "post_balances": {"USDC": "20.5", "WETH": "0.001"},
        "incident": False,
        "reconciliation_block": 472432523,
        "reconciliation_degraded": False,
        "reconciliation_pre_anchored": False,
        "reconciliation_confirmed": None,  # no wait ran — None preserved (Empty != Zero)
        "reconciliation_confirmation_depth": 0,
        "reconciliation_head_block": None,
    }
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert out["reconciliation_block"] == 472432523
    assert out["reconciliation_degraded"] is False
    assert out["reconciliation_pre_anchored"] is False
    assert "reconciliation_confirmed" in out and out["reconciliation_confirmed"] is None
    assert out["reconciliation_confirmation_depth"] == 0


def test_post_state_omits_block_anchoring_flags_for_legacy_recon():
    """A legacy recon dict without the VIB-3350 flags gains no spurious keys."""
    recon = {"post_balances": {"USDC": "1"}, "incident": False}
    out = _build_post_state_for_ledger(recon)
    assert out is not None
    assert "reconciliation_block" not in out
    assert "reconciliation_degraded" not in out


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
    # VIB-4929 PR-3a: Aave + Morpho share the unified LendingAccountState; the
    # Aave-family discriminator (family="aave") is what the serializer gates the
    # Aave-only keys on.
    from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

    return LendingAccountState(
        collateral_usd=Decimal(collateral_usd),
        debt_usd=Decimal(debt_usd),
        health_factor=Decimal(hf),
        liquidation_threshold_bps=lt_bps,
        e_mode_category=None,
        family="aave",
    )


def _morpho_state(collateral_usd: str, debt_usd: str, hf: str, lltv: str):
    from almanak.connectors._strategy_base.lending_read_base import LendingAccountState

    return LendingAccountState(
        collateral_usd=Decimal(collateral_usd),
        debt_usd=Decimal(debt_usd),
        health_factor=Decimal(hf),
        liquidation_threshold_bps=None,
        e_mode_category=None,
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
    """SUPPLY on Aave V3 with a working gateway returns a LendingAccountState.

    Anti-regression contract for VIB-3474 / iter-176 VIB-2986 shape: the
    gateway-backed flow MUST exercise the populate path. The previous bug
    let the legacy validation skip silently when ``rpc_url=None`` was passed
    alongside a gateway_client. This test asserts the ``capture_lending_*``
    helpers still reach the gateway when the runner threads it through.

    VIB-4929 PR-3a: the Aave read returns the unified LendingAccountState (the
    per-protocol AaveAccountState is gone) — fields are unchanged.
    """
    from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
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
    assert isinstance(out, LendingAccountState)
    assert out.family == "aave"  # structural discriminator the serializer gates Aave keys on
    assert out.collateral_usd == Decimal("15420.50")
    assert out.debt_usd == Decimal("8200.00")
    assert out.liquidation_threshold_bps == 8500
    assert out.health_factor == Decimal("1.882")
    # The gateway WAS called — the populate path is exercised, not silently
    # skipped. VIB-4213 adds a second eth_call for `Pool.getUserEMode` so
    # `e_mode_category` lands in pre/post_state_json; the count is now 2.
    assert len(gateway.calls) == 2
    # First call selector is `getUserAccountData` (0xbf92857c).
    assert gateway.calls[0][2].startswith("0xbf92857c")
    # Second call selector is `getUserEMode` (0xeddf1b79, VIB-4213).
    assert gateway.calls[1][2].startswith("0xeddf1b79")


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
        "deployment_id": "s",
        "cycle_id": "c",
        "wallet_address": "0x" + "a" * 40,
        "position_key": "lending:ethereum:aave_v3:0x" + "a" * 40 + ":USDC",
        "market_id": "",
    }
    ledger = {
        "id": led_id,
        "intent_type": "SUPPLY",
        "deployment_id": "d",
        "deployment_id": "s",
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


# ---------------------------------------------------------------------------
# VIB-4482 (P-V1-A) — _capture_v4_lp_close_fees_safe: PRE-close V4 fee read.
#
# Uniswap V4 bundles fees into the withdrawal Transfer, so the close receipt
# cannot separate them; the runner reads tokens_owed0/1 on-chain BEFORE the burn
# (a post-burn read returns zero liquidity) via the connector-owned reader hook
# (gateway QueryV4PositionState RPC). Returns the raw-int pair or None — never
# fabricates a zero (Empty != Zero).
# ---------------------------------------------------------------------------


def _v4_close_intent(intent_type="LP_CLOSE", protocol="uniswap_v4", position_id="12345"):
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        protocol=protocol,
        position_id=position_id,
    )


def test_capture_v4_lp_close_fees_returns_none_without_gateway():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    out = StrategyRunner._capture_v4_lp_close_fees_safe(
        intent=_v4_close_intent(),
        chain="base",
        gateway_client=None,
    )
    assert out is None


def test_capture_v4_lp_close_fees_skips_non_v4_protocol():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    out = StrategyRunner._capture_v4_lp_close_fees_safe(
        intent=_v4_close_intent(protocol="uniswap_v3"),
        chain="base",
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0


def test_capture_v4_lp_close_fees_skips_non_close_intents():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    for intent_type in ("LP_OPEN", "SWAP", "SUPPLY", "PERP_OPEN"):
        out = StrategyRunner._capture_v4_lp_close_fees_safe(
            intent=_v4_close_intent(intent_type=intent_type),
            chain="base",
            gateway_client=gateway,
        )
        assert out is None
    assert gateway.calls == 0


def test_capture_v4_lp_close_fees_skips_missing_token_id():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    # None / "" / 0 / non-positive are not usable NFT tokenIds.
    for pid in (None, "", "0", "-5", "abc"):
        out = StrategyRunner._capture_v4_lp_close_fees_safe(
            intent=_v4_close_intent(position_id=pid),
            chain="base",
            gateway_client=gateway,
        )
        assert out is None
    assert gateway.calls == 0


def test_capture_v4_lp_close_fees_reads_owed_pair_on_clean_read():
    """A clean gateway read returns (tokens_owed0, tokens_owed1) as ints."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def __init__(self):
            self.calls: list[dict] = []

        def query_v4_position_state(self, *, chain, position_manager, state_view, token_id):
            self.calls.append(
                {
                    "chain": chain,
                    "position_manager": position_manager,
                    "state_view": state_view,
                    "token_id": token_id,
                }
            )
            return SimpleNamespace(tokens_owed0=4242, tokens_owed1=2424)

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_close_fees_safe(
        intent=_v4_close_intent(position_id="12345"),
        chain="base",
        gateway_client=gateway,
    )
    assert out == (4242, 2424)
    # The real reader hook resolved base's PositionManager + StateView and
    # threaded the tokenId through.
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["chain"] == "base"
    assert gateway.calls[0]["token_id"] == 12345
    assert gateway.calls[0]["state_view"]  # connector-resolved, non-empty


def test_capture_v4_lp_close_fees_measured_zero_preserved():
    """tokens_owed == 0 is a MEASURED zero (Empty != Zero) — returned as 0, not None."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            return SimpleNamespace(tokens_owed0=0, tokens_owed1=0)

    out = StrategyRunner._capture_v4_lp_close_fees_safe(
        intent=_v4_close_intent(),
        chain="base",
        gateway_client=_Gateway(),
    )
    assert out == (0, 0)


def test_capture_v4_lp_close_fees_none_state_returns_none():
    """A failed / partial gateway read (reader returns None) => None (unmeasured)."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            return None  # reader fails closed → None

    out = StrategyRunner._capture_v4_lp_close_fees_safe(
        intent=_v4_close_intent(),
        chain="base",
        gateway_client=_Gateway(),
    )
    assert out is None


def test_capture_v4_lp_close_fees_undeployed_chain_returns_none():
    """A chain with no V4 StateView address => reader returns None => None."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, **__):
            self.calls += 1
            return SimpleNamespace(tokens_owed0=1, tokens_owed1=2)

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_close_fees_safe(
        intent=_v4_close_intent(),
        chain="zzz_nonexistent_chain",
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0  # reader short-circuits before the RPC


# ---------------------------------------------------------------------------
# VIB-4483 (P-V1-B) — _capture_v4_lp_open_native_amounts_safe: POST-mint native
# leg read. A native-ETH V4 pool deposits its ETH leg via msg.value (no ERC-20
# Transfer), so the receipt parser leaves that leg None. The runner reads the
# freshly-minted position state and derives (amount0, amount1) via the
# framework's concentrated-liquidity math. Returns the raw-int pair or None —
# never fabricates a zero (Empty != Zero). Symmetric with the close-fee read.
# ---------------------------------------------------------------------------

_NATIVE = "0x0000000000000000000000000000000000000000"
_USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _v4_open_result(
    *,
    currency0=_NATIVE,
    currency1=_USDC_BASE,
    position_id=4242,
    amount0=None,
    amount1=1_000_000_000,
    protocol_attr=True,
):
    """An enriched result carrying a native-pool LPOpenData (typed attr path)."""
    lp_open = SimpleNamespace(
        position_id=position_id,
        currency0=currency0,
        currency1=currency1,
        amount0=amount0,
        amount1=amount1,
    )
    return SimpleNamespace(lp_open_data=lp_open, extracted_data={"lp_open_data": lp_open})


def _v4_open_intent(intent_type="LP_OPEN", protocol="uniswap_v4"):
    return SimpleNamespace(intent_type=SimpleNamespace(value=intent_type), protocol=protocol)


def test_capture_v4_open_native_returns_none_without_gateway():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=_v4_open_result(),
        gateway_client=None,
    )
    assert out is None


def test_capture_v4_open_native_skips_non_v4_protocol():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(protocol="uniswap_v3"),
        chain="base",
        result=_v4_open_result(),
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0


def test_capture_v4_open_native_skips_non_open_intents():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    for intent_type in ("LP_CLOSE", "SWAP", "SUPPLY", "LP_COLLECT_FEES"):
        out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
            intent=_v4_open_intent(intent_type=intent_type),
            chain="base",
            result=_v4_open_result(),
            gateway_client=gateway,
        )
        assert out is None
    assert gateway.calls == 0


def test_capture_v4_open_native_skips_erc20_only_pool():
    """An ERC20-ERC20 pool (neither leg native) needs no gateway read."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    weth = "0x4200000000000000000000000000000000000006"
    gateway = _GatewayProbe()
    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=_v4_open_result(currency0=weth, currency1=_USDC_BASE),
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0


def test_capture_v4_open_native_skips_missing_position_id():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    for pid in (None, 0, -5):
        out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
            intent=_v4_open_intent(),
            chain="base",
            result=_v4_open_result(position_id=pid),
            gateway_client=gateway,
        )
        assert out is None
    assert gateway.calls == 0


def test_capture_v4_open_native_derives_amounts_on_clean_read():
    """A clean read derives (amount0, amount1) raw ints via the framework math."""
    from almanak.framework.runner.strategy_runner import StrategyRunner
    from almanak.framework.valuation.lp_valuer import get_token_amounts_from_sqrt_price

    # In-range position: tick_lower < current < tick_upper → both legs positive.
    tick_lower, tick_upper = -887220, 887220
    liquidity = 10**15
    # sqrtPriceX96 at tick ~0 (price ~1.0): 2**96.
    sqrt_price_x96 = 2**96

    class _Gateway:
        def __init__(self):
            self.calls: list[dict] = []

        def query_v4_position_state(self, *, chain, position_manager, state_view, token_id):
            self.calls.append({"chain": chain, "token_id": token_id, "state_view": state_view})
            return SimpleNamespace(
                liquidity=liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                current_tick=0,
                sqrt_price_x96=sqrt_price_x96,
            )

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=_v4_open_result(),
        gateway_client=gateway,
    )
    expected = get_token_amounts_from_sqrt_price(liquidity, tick_lower, tick_upper, sqrt_price_x96)
    assert out == (int(expected.amount0), int(expected.amount1))
    assert out[0] > 0, "native (currency0) leg must be a positive derived deposit"
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["chain"] == "base"
    assert gateway.calls[0]["token_id"] == 4242
    assert gateway.calls[0]["state_view"]  # connector-resolved, non-empty


def test_capture_v4_open_native_none_state_returns_none():
    """A failed/partial read (reader returns None) => None (unmeasured, never 0)."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            return None

    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=_v4_open_result(),
        gateway_client=_Gateway(),
    )
    assert out is None


def test_capture_v4_open_native_undeployed_chain_returns_none():
    """A chain with no V4 StateView address => reader short-circuits => None."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, **__):
            self.calls += 1
            return SimpleNamespace(liquidity=1, tick_lower=-1, tick_upper=1, current_tick=0, sqrt_price_x96=2**96)

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="zzz_nonexistent_chain",
        result=_v4_open_result(),
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0


def test_capture_v4_open_native_resolves_dict_shaped_lp_open_data():
    """The native gate must fire when lp_open_data is the serialised DICT shape.

    ``_result_lp_open_data`` may return ``extracted_data['lp_open_data']`` as a
    dict (post-serialisation). A bare ``getattr`` on a dict yields ``None`` and
    would silently skip native capture — the gate reads via ``_lp_open_field``
    so both the dataclass and dict shapes resolve (VIB-4483, CodeRabbit review).
    """
    from almanak.framework.runner.strategy_runner import StrategyRunner
    from almanak.framework.valuation.lp_valuer import get_token_amounts_from_sqrt_price

    tick_lower, tick_upper, liquidity, sqrt_price_x96 = -887220, 887220, 10**15, 2**96
    # Result whose ONLY lp-open surface is the dict (no typed attr).
    lp_open_dict = {
        "position_id": 4242,
        "currency0": _NATIVE,
        "currency1": _USDC_BASE,
        "amount0": None,
        "amount1": 1_000_000_000,
        "tick_lower": tick_lower,
        "tick_upper": tick_upper,
    }
    result = SimpleNamespace(lp_open_data=None, extracted_data={"lp_open_data": lp_open_dict})

    class _Gateway:
        def query_v4_position_state(self, *, chain, position_manager, state_view, token_id):
            return SimpleNamespace(
                liquidity=liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                current_tick=0,
                sqrt_price_x96=sqrt_price_x96,
            )

    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=result,
        gateway_client=_Gateway(),
    )
    expected = get_token_amounts_from_sqrt_price(liquidity, tick_lower, tick_upper, sqrt_price_x96)
    assert out == (int(expected.amount0), int(expected.amount1))


def test_capture_v4_open_native_reader_raising_does_not_crash_runner():
    """A reader raising a NON-socket exception must be swallowed → None.

    This is a best-effort SUCCESS-path hook: the trade already landed on-chain,
    so an RPC-layer error (e.g. ContractLogicError / ValueError / ABI decode)
    must never propagate and crash the runner (Gemini review, VIB-4483).
    """
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            raise ValueError("could not decode position state response")

    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=_v4_open_result(),
        gateway_client=_Gateway(),
    )
    assert out is None


def test_capture_v4_open_native_malformed_amounts_does_not_crash_runner():
    """Derivation/coercion failures are swallowed → None (never crash, never 0).

    If the read returns state whose ``.amount0`` cannot be coerced to int (the
    math/coercion is now inside the guard), the hook returns None rather than
    propagating an AttributeError/ZeroDivisionError onto the success path.
    """
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            # Non-numeric liquidity → int(liquidity) raises inside the guard.
            return SimpleNamespace(
                liquidity="not-a-number",
                tick_lower=-1,
                tick_upper=1,
                current_tick=0,
                sqrt_price_x96=2**96,
            )

    out = StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=_v4_open_intent(),
        chain="base",
        result=_v4_open_result(),
        gateway_client=_Gateway(),
    )
    assert out is None


# ---------------------------------------------------------------------------
# VIB-5117 — _capture_v4_lp_close_native_principal_safe: PRE-burn native-leg
# close PRINCIPAL read. A native-ETH V4 leg is returned to the wallet as raw ETH
# (TAKE_PAIR, no Transfer), so the burn receipt leaves amount{0,1}_collected None
# on that leg. The runner derives the principal from the SAME pre-burn position
# state the fee capture uses (liquidity + sqrt_price + ticks → concentrated-
# liquidity math). Gated on the close INTENT's native-leg protocol_params (the
# LPCloseData does not exist pre-burn). Returns the raw-int pair or None — never
# fabricates a zero (Empty != Zero). Close-side mirror of the open capture.
# ---------------------------------------------------------------------------

_NATIVE_5117 = "0x0000000000000000000000000000000000000000"
_USDC_BASE_5117 = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _v4_close_intent_native(
    *,
    intent_type="LP_CLOSE",
    protocol="uniswap_v4",
    position_id="12345",
    currency0=_NATIVE_5117,
    currency1=_USDC_BASE_5117,
    with_params=True,
    liquidity=None,
):
    params = {"currency0": currency0, "currency1": currency1} if with_params else None
    if params is not None and liquidity is not None:
        # The strategy requests a PARTIAL close by setting protocol_params["liquidity"]
        # below the full position liquidity (uniswap_v4/compiler.py reads it verbatim).
        params["liquidity"] = liquidity
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        protocol=protocol,
        position_id=position_id,
        protocol_params=params,
    )


def test_capture_v4_close_principal_returns_none_without_gateway():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(),
        chain="base",
        gateway_client=None,
    )
    assert out is None


def test_capture_v4_close_principal_no_protocol_params_still_reads():
    """The capture does NOT depend on intent.protocol_params (the real-path bug).

    The teardown / strategy LP_CLOSE intent carries no ``protocol_params`` (only
    the compiler resolves currencies, internal to the action bundle). The
    pre-burn read must still fire and derive the principal — the stamp's never-
    clobber-None-only guard is the safety, not a pre-read pool-shape gate.
    """
    from almanak.framework.runner.strategy_runner import StrategyRunner
    from almanak.framework.valuation.lp_valuer import get_token_amounts_from_sqrt_price

    tick_lower, tick_upper, liquidity, sqrt_price_x96 = -887220, 887220, 10**15, 2**96

    class _Gateway:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *, chain, position_manager, state_view, token_id):
            self.calls += 1
            return SimpleNamespace(
                liquidity=liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                current_tick=0,
                sqrt_price_x96=sqrt_price_x96,
            )

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(with_params=False),
        chain="base",
        gateway_client=gateway,
    )
    expected = get_token_amounts_from_sqrt_price(liquidity, tick_lower, tick_upper, sqrt_price_x96)
    assert out == (int(expected.amount0), int(expected.amount1))
    assert gateway.calls == 1  # the read fires even without protocol_params


def test_capture_v4_close_principal_skips_non_v4_protocol():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(protocol="uniswap_v3"),
        chain="base",
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0


def test_capture_v4_close_principal_skips_non_close_intents():
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _GatewayProbe:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, *_, **__):
            self.calls += 1
            return None

    gateway = _GatewayProbe()
    for intent_type in ("LP_OPEN", "SWAP", "SUPPLY", "PERP_OPEN"):
        out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
            intent=_v4_close_intent_native(intent_type=intent_type),
            chain="base",
            gateway_client=gateway,
        )
        assert out is None
    assert gateway.calls == 0


def test_capture_v4_close_principal_derives_amounts_on_clean_read():
    """A clean pre-burn read derives (amount0, amount1) raw ints via framework math."""
    from almanak.framework.runner.strategy_runner import StrategyRunner
    from almanak.framework.valuation.lp_valuer import get_token_amounts_from_sqrt_price

    tick_lower, tick_upper = -887220, 887220
    liquidity = 10**15
    sqrt_price_x96 = 2**96  # price ~1.0

    class _Gateway:
        def __init__(self):
            self.calls: list[dict] = []

        def query_v4_position_state(self, *, chain, position_manager, state_view, token_id):
            self.calls.append({"chain": chain, "token_id": token_id, "state_view": state_view})
            return SimpleNamespace(
                liquidity=liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                current_tick=0,
                sqrt_price_x96=sqrt_price_x96,
                tokens_owed0=0,
                tokens_owed1=0,
            )

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(position_id="12345"),
        chain="base",
        gateway_client=gateway,
    )
    expected = get_token_amounts_from_sqrt_price(liquidity, tick_lower, tick_upper, sqrt_price_x96)
    assert out == (int(expected.amount0), int(expected.amount1))
    assert out[0] > 0, "native (currency0) principal leg must be a positive derived amount"
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["chain"] == "base"
    assert gateway.calls[0]["token_id"] == 12345
    assert gateway.calls[0]["state_view"]  # connector-resolved, non-empty


def test_capture_v4_close_principal_partial_close_uses_requested_liquidity():
    """VIB-5117 (Codex P1): a PARTIAL native close derives the principal from the
    REQUESTED liquidity (protocol_params["liquidity"]), not the full pre-burn
    position liquidity — else proceeds/PnL overstate by the unburned fraction.
    """
    from almanak.framework.runner.strategy_runner import StrategyRunner
    from almanak.framework.valuation.lp_valuer import get_token_amounts_from_sqrt_price

    tick_lower, tick_upper = -887220, 887220
    full_liquidity = 10**15
    requested_liquidity = full_liquidity // 4  # close only a quarter
    sqrt_price_x96 = 2**96

    class _Gateway:
        def query_v4_position_state(self, *, chain, position_manager, state_view, token_id):
            # The on-chain read always reports the FULL pre-burn position liquidity.
            return SimpleNamespace(
                liquidity=full_liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                current_tick=0,
                sqrt_price_x96=sqrt_price_x96,
            )

    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(liquidity=requested_liquidity),
        chain="base",
        gateway_client=_Gateway(),
    )
    expected_requested = get_token_amounts_from_sqrt_price(
        requested_liquidity, tick_lower, tick_upper, sqrt_price_x96
    )
    expected_full = get_token_amounts_from_sqrt_price(
        full_liquidity, tick_lower, tick_upper, sqrt_price_x96
    )
    assert out == (int(expected_requested.amount0), int(expected_requested.amount1))
    # Guard against the bug this test exists for: the derived principal must be the
    # requested quarter, strictly less than the full-position principal.
    assert out[0] < int(expected_full.amount0)
    assert out[0] > 0


def test_capture_v4_close_principal_full_close_ignores_absent_liquidity_param():
    """A full close (no protocol_params["liquidity"]) keeps the full pre-burn read."""
    from almanak.framework.runner.strategy_runner import StrategyRunner
    from almanak.framework.valuation.lp_valuer import get_token_amounts_from_sqrt_price

    tick_lower, tick_upper = -887220, 887220
    full_liquidity = 10**15
    sqrt_price_x96 = 2**96

    class _Gateway:
        def query_v4_position_state(self, **__):
            return SimpleNamespace(
                liquidity=full_liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                current_tick=0,
                sqrt_price_x96=sqrt_price_x96,
            )

    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(),  # no liquidity param → full close
        chain="base",
        gateway_client=_Gateway(),
    )
    expected_full = get_token_amounts_from_sqrt_price(
        full_liquidity, tick_lower, tick_upper, sqrt_price_x96
    )
    assert out == (int(expected_full.amount0), int(expected_full.amount1))


def test_capture_v4_close_principal_none_state_returns_none():
    """A failed / partial read (reader returns None) => None (unmeasured, never 0)."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            return None

    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(),
        chain="base",
        gateway_client=_Gateway(),
    )
    assert out is None


def test_capture_v4_close_principal_undeployed_chain_returns_none():
    """A chain with no V4 StateView address => reader short-circuits => None."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def __init__(self):
            self.calls = 0

        def query_v4_position_state(self, **__):
            self.calls += 1
            return SimpleNamespace(
                liquidity=1, tick_lower=-1, tick_upper=1, current_tick=0, sqrt_price_x96=2**96
            )

    gateway = _Gateway()
    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(),
        chain="zzz_nonexistent_chain",
        gateway_client=gateway,
    )
    assert out is None
    assert gateway.calls == 0


def test_capture_v4_close_principal_degenerate_state_returns_none():
    """Non-numeric state inside the derivation guard returns None (never crashes)."""
    from almanak.framework.runner.strategy_runner import StrategyRunner

    class _Gateway:
        def query_v4_position_state(self, **__):
            return SimpleNamespace(
                liquidity="not-a-number",
                tick_lower=-1,
                tick_upper=1,
                current_tick=0,
                sqrt_price_x96=2**96,
            )

    out = StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=_v4_close_intent_native(),
        chain="base",
        gateway_client=_Gateway(),
    )
    assert out is None
