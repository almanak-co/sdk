"""VIB-5983 — position_events value_usd mis-scaled when config pool-label
order disagrees with on-chain token0()/token1() (address) order.

Sibling of VIB-5851 (accounting_events / lp_handler). Same defect class,
different producer: Layer-3 position_events pairs receipt amount0/amount1
(on-chain address-sorted) with token symbols from the intent pool label
(\"WETH/USDC/500\"). On Ethereum the pool is USDC-first; the WETH-shaped raw
amount is scaled with USDC decimals and priced at $1 → ~$1.67bn phantom
value_usd on a ~$6 LP.

These tests exercise BOTH orderings:

* label != chain (Ethereum-like: USDC addr < WETH) — must realign + sane value
* label == chain (Arbitrum-like: WETH addr < USDC) — no collateral movement
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.data.tokens.exceptions import TokenNotFoundError

from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.position_events import (
    build_position_event_from_intent,
)

# G6 / VIB-5851 magnitudes: on-chain amount0 = USDC raw, amount1 = WETH raw
# when the pool is USDC-first.
USDC_RAW = 2_185_779  # 6 dec → 2.185779 USDC
WETH_RAW = 1_032_114_889_479_681  # 18 dec → ~0.001032 WETH

WETH_PRICE = "1917.0"
USDC_PRICE = "1.0"

EXPECTED_VALUE_LOW = Decimal("3.0")
EXPECTED_VALUE_HIGH = Decimal("6.0")
# Pre-fix phantom: WETH raw / 1e6 * $1 ≈ 1.03e9 (and larger with real proof amounts)
PHANTOM_FLOOR = Decimal("1_000_000")


def _addr(byte: str) -> str:
    return "0x" + byte * 20


_ADDR_BOOKS = {
    "eth_like": {"USDC": _addr("11"), "WETH": _addr("cc")},  # USDC < WETH → chain order USDC,WETH
    "arb_like": {"WETH": _addr("11"), "USDC": _addr("cc")},  # WETH < USDC → chain order WETH,USDC
}
_DECIMALS = {"USDC": 6, "WETH": 18}


def _patch_resolver(monkeypatch, book_key: str) -> None:
    book = _ADDR_BOOKS[book_key]

    class _FakeResolver:
        # VIB-6628: accepts kwargs loosely rather than mirroring production's exact
        # acceptance surface. Conformance (does it accept every legal call?) is
        # enforced by test_resolver_double_conformance_vib6100.py; strictness (does
        # it reject illegal ones?) is tracked there. Tightening needs the surface
        # MEASURED first — a double stricter than production is a false-green
        # generator too, as the chain-alias case in #3472 showed.
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ANN001, ARG002
            up = str(token).upper()
            if up in book:
                return SimpleNamespace(symbol=up, address=book[up], decimals=_DECIMALS[up], chain=chain)
            for sym, addr in book.items():
                if addr.lower() == str(token).lower():
                    return SimpleNamespace(symbol=sym, address=addr, decimals=_DECIMALS[sym], chain=chain)
            raise TokenNotFoundError(token=str(token), chain=str(chain), reason="unknown")

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _FakeResolver(),
    )


def _price_oracle() -> dict:
    return {"WETH": WETH_PRICE, "USDC": USDC_PRICE}


class _Intent:
    def __init__(
        self,
        intent_type: str,
        pool: str = "WETH/USDC/500",
        protocol: str = "uniswap_v3",
        *,
        from_token: str | None = "WETH",
        to_token: str | None = "USDC",
    ):
        self.intent_type = type("IT", (), {"value": intent_type})()
        self.protocol = protocol
        self.pool = pool
        self.position_id = "42"
        # Label-order sides when set. Pass from_token=None/to_token=None to force
        # the pool-descriptor fallback path in _pair_tokens_from_intent (VIB-5983 CR).
        self.from_token = from_token
        self.to_token = to_token
        self.token0 = None
        self.token1 = None


class _Result:
    def __init__(self, extracted: dict, tx_hash: str = "0xopen"):
        self.position_id = "42"
        self.transaction_results = [SimpleNamespace(tx_hash=tx_hash, gas_used=200000, success=True)]
        self.gas_cost_usd = "1.00"
        self.extracted_data = extracted


# ── pure helper ──────────────────────────────────────────────────────────


def test_realign_swaps_when_label_order_inverted(monkeypatch):
    _patch_resolver(monkeypatch, "eth_like")
    t0, t1 = realign_token_pair_by_address("WETH", "USDC", "ethereum")
    assert (t0, t1) == ("USDC", "WETH")


def test_realign_noop_when_label_already_chain_order(monkeypatch):
    _patch_resolver(monkeypatch, "arb_like")
    t0, t1 = realign_token_pair_by_address("WETH", "USDC", "arbitrum")
    assert (t0, t1) == ("WETH", "USDC")


def test_realign_fail_open_on_unresolved(monkeypatch):
    class _Empty:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):
            raise TokenNotFoundError(token=str(token), chain=str(chain), reason="empty")

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Empty(),
    )
    assert realign_token_pair_by_address("WETH", "USDC", "ethereum") == ("WETH", "USDC")


# ── OPEN path ────────────────────────────────────────────────────────────


def test_open_eth_like_realigns_tokens_and_sane_value_usd(monkeypatch):
    """Label WETH/USDC + USDC-first chain order → tokens realign, value ~$4 not $1bn."""
    _patch_resolver(monkeypatch, "eth_like")
    lp_open = LPOpenData(
        position_id=42,
        tick_lower=-60000,
        tick_upper=60000,
        liquidity=500_000,
        amount0=USDC_RAW,  # on-chain token0
        amount1=WETH_RAW,  # on-chain token1
        pool_address="0x" + "ab" * 20,
    )
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_OPEN"),
        result=_Result({"lp_open_data": lp_open}),
        chain="ethereum",
        price_oracle=_price_oracle(),
    )
    assert event is not None
    assert event.token0.upper() == "USDC"
    assert event.token1.upper() == "WETH"
    assert event.value_usd != ""
    val = Decimal(event.value_usd)
    assert EXPECTED_VALUE_LOW < val < EXPECTED_VALUE_HIGH, f"expected ~$4, got {val}"
    assert val < PHANTOM_FLOOR


def test_open_eth_like_without_realign_would_be_phantom(monkeypatch):
    """Mutation check: label-order symbols + chain-order amounts → phantom value.

    Documents the pre-fix failure mode by calling value_usd helper with the
    WRONG pairing (what the producer used to emit).
    """
    from almanak.framework.observability.position_events import (
        PositionEvent,
        _apply_lp_open_value_usd,
    )

    _patch_resolver(monkeypatch, "eth_like")
    event = PositionEvent(
        deployment_id="d",
        position_id="42",
        position_type="LP",
        event_type="OPEN",
        protocol="uniswap_v3",
        chain="ethereum",
        token0="WETH",  # wrong: label order
        token1="USDC",
        amount0=str(USDC_RAW),
        amount1=str(WETH_RAW),
    )
    _apply_lp_open_value_usd(event, _price_oracle(), chain="ethereum")
    assert event.value_usd != ""
    assert Decimal(event.value_usd) > PHANTOM_FLOOR


def test_open_arb_like_unchanged(monkeypatch):
    """When label order already matches chain order, tokens and value stay correct."""
    _patch_resolver(monkeypatch, "arb_like")
    # On arb-like, on-chain token0=WETH, token1=USDC — amounts must match that order.
    lp_open = LPOpenData(
        position_id=42,
        tick_lower=-60000,
        tick_upper=60000,
        liquidity=500_000,
        amount0=WETH_RAW,
        amount1=USDC_RAW,
        pool_address="0x" + "ab" * 20,
    )
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_OPEN"),
        result=_Result({"lp_open_data": lp_open}),
        chain="arbitrum",
        price_oracle=_price_oracle(),
    )
    assert event is not None
    assert event.token0.upper() == "WETH"
    assert event.token1.upper() == "USDC"
    val = Decimal(event.value_usd)
    assert EXPECTED_VALUE_LOW < val < EXPECTED_VALUE_HIGH


# ── CLOSE path ───────────────────────────────────────────────────────────


def test_close_eth_like_realigns_from_intent_pool(monkeypatch):
    """CLOSE without OPEN cache: pool label fallback must still realign."""
    _patch_resolver(monkeypatch, "eth_like")
    lp_close = LPCloseData(
        amount0_collected=USDC_RAW,
        amount1_collected=WETH_RAW,
        fees0=None,
        fees1=None,
        liquidity_removed=500_000,
        pool_address="0x" + "ab" * 20,
    )
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_CLOSE", from_token=None, to_token=None),
        result=_Result({"lp_close_data": lp_close}, tx_hash="0xclose"),
        chain="ethereum",
        price_oracle=_price_oracle(),
        recent_open_events=None,
    )
    assert event is not None
    assert event.event_type == "CLOSE"
    # Tokens must come from pool="WETH/USDC/500" parse, then realign to chain order.
    assert event.token0.upper() == "USDC"
    assert event.token1.upper() == "WETH"
    val = Decimal(event.value_usd)
    assert EXPECTED_VALUE_LOW < val < EXPECTED_VALUE_HIGH
    assert val < PHANTOM_FLOOR


def test_close_realigns_even_when_cache_carries_label_order(monkeypatch):
    """Pre-fix OPEN cache may store label-order tokens; CLOSE must still realign."""
    _patch_resolver(monkeypatch, "eth_like")
    lp_close = LPCloseData(
        amount0_collected=USDC_RAW,
        amount1_collected=WETH_RAW,
        fees0=None,
        fees1=None,
        liquidity_removed=500_000,
        pool_address="0x" + "ab" * 20,
    )
    cache = {
        ("42", "LP"): {
            "tick_lower": -60000,
            "tick_upper": 60000,
            "liquidity": "500000",
            "token0": "WETH",  # poisoned label order from pre-fix OPEN
            "token1": "USDC",
        }
    }
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_CLOSE"),
        result=_Result({"lp_close_data": lp_close}, tx_hash="0xclose"),
        chain="ethereum",
        price_oracle=_price_oracle(),
        recent_open_events=cache,
    )
    assert event is not None
    assert event.token0.upper() == "USDC"
    assert event.token1.upper() == "WETH"
    assert EXPECTED_VALUE_LOW < Decimal(event.value_usd) < EXPECTED_VALUE_HIGH


def test_ncoin_coin_symbols_skips_realign(monkeypatch):
    """Fungible N-coin path must not address-sort (pool-index order)."""
    _patch_resolver(monkeypatch, "eth_like")
    lp_open = LPOpenData(
        position_id=42,
        tick_lower=None,
        tick_upper=None,
        liquidity=0,
        amount0=USDC_RAW,
        amount1=WETH_RAW,
        pool_address="0x" + "ab" * 20,
        coin_symbols=["WETH", "USDC", "DAI"],
    )
    event = build_position_event_from_intent(
        deployment_id="d1",
        intent=_Intent("LP_OPEN", pool="WETH/USDC/DAI"),
        result=_Result({"lp_open_data": lp_open}),
        chain="ethereum",
        price_oracle=_price_oracle(),
    )
    assert event is not None
    # Label order preserved (realign gated off) — first two from pool parse.
    assert event.token0.upper() == "WETH"
    assert event.token1.upper() == "USDC"
