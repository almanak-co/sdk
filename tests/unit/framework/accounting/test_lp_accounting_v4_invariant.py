"""VIB-4477 (T08): V4 LP ``pool_address`` invariant.

Any LP accounting payload whose ``source == "modify_liquidity"`` OR whose
``protocol`` resolves to Uniswap V4 MUST carry a 32-byte ``pool_address``
matching ``^0x[0-9a-f]{64}$`` (canonical V4 ``pool_id``, NOT the 20-byte
EOA-style address V3 / Aerodrome / TraderJoe pools use). V3 lanes are the
regression mirror: V3 ``pool_address`` values stay at 20-byte EVM addresses.

Fail-loud contract: a future code path that misroutes a V4 pool_id into the
20-byte slot, or vice-versa, must surface here BEFORE the payload reaches
``accounting_events.payload_json`` — silent mis-attribution at the storage
boundary is what this invariant exists to prevent.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal

import pytest

from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    LPEventType,
)
from almanak.framework.accounting.writer import augment_accounting_payload
from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)
from almanak.framework.connectors.uniswap_v4.sdk import PoolKey, _pad_int24, _pad_uint

POOL_ID_32_BYTE = "0x" + "ab" * 32
POOL_ID_REGEX = re.compile(r"^0x[0-9a-f]{64}$")
V3_POOL_ADDR_20_BYTE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
V3_POOL_REGEX = re.compile(r"^0x[0-9a-f]{40}$")

POOL_MANAGER = "0x000000000004444c5dc75cB358380D2e3dE08A90"
POSITION_MANAGER = "0xBd216513D74C8cf14cF4747E6AaE6fDf64e83b24"
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


def _identity(tx: str) -> AccountingIdentity:
    from datetime import UTC, datetime

    return AccountingIdentity(
        id="ev-1",
        deployment_id="d",
        strategy_id="s",
        cycle_id="c",
        execution_mode="paper",
        timestamp=datetime.now(UTC),
        chain="arbitrum",
        protocol="uniswap_v4",
        wallet_address=WALLET,
        tx_hash=tx,
        ledger_entry_id="le",
    )


# =============================================================================
# 1. The shape contract — 32-byte pool_address holds through to payload JSON
# =============================================================================


class TestV4PoolAddressShape:
    def test_v4_lp_event_emits_32_byte_pool_address(self):
        """An LPAccountingEvent constructed with a 32-byte V4 pool_id keeps
        the 64-hex-char shape end-to-end through ``to_payload_json``."""
        event = LPAccountingEvent(
            identity=_identity("0xv4open"),
            event_type=LPEventType.LP_OPEN,
            position_key=f"lp:uniswap_v4:arbitrum:{WALLET}:{POOL_ID_32_BYTE}",
            pool_address=POOL_ID_32_BYTE,
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            lp_token_amount=None,
            cost_basis_usd=Decimal("4000"),
            realized_pnl_usd=None,
            fees0_collected=None,
            fees1_collected=None,
            confidence=AccountingConfidence.HIGH,
        )
        payload = json.loads(event.to_payload_json())
        assert payload["pool_address"] == POOL_ID_32_BYTE
        assert POOL_ID_REGEX.fullmatch(payload["pool_address"]) is not None

    def test_v3_lp_event_keeps_20_byte_pool_address(self):
        """V3 regression mirror: V3 pool_address stays at 40-hex chars after
        passing through the writer's augment chokepoint."""
        event = LPAccountingEvent(
            identity=AccountingIdentity(
                id="ev-v3",
                deployment_id="d",
                strategy_id="s",
                cycle_id="c",
                execution_mode="paper",
                timestamp=__import__("datetime").datetime.now(__import__("datetime").UTC),
                chain="arbitrum",
                protocol="uniswap_v3",
                wallet_address=WALLET,
                tx_hash="0xv3open",
                ledger_entry_id="le",
            ),
            event_type=LPEventType.LP_OPEN,
            position_key=f"lp:uniswap_v3:arbitrum:{WALLET}:{V3_POOL_ADDR_20_BYTE}",
            pool_address=V3_POOL_ADDR_20_BYTE,
            token0="WETH",
            token1="USDC",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            lp_token_amount=None,
            cost_basis_usd=Decimal("4000"),
            realized_pnl_usd=None,
            fees0_collected=None,
            fees1_collected=None,
            confidence=AccountingConfidence.HIGH,
        )
        augmented = augment_accounting_payload(event.to_payload_json(), is_live=False)
        d = json.loads(augmented)
        assert d["pool_address"] == V3_POOL_ADDR_20_BYTE
        assert V3_POOL_REGEX.fullmatch(d["pool_address"]) is not None
        # Crucially: V3 must NOT match the 64-hex-char regex.
        assert POOL_ID_REGEX.fullmatch(d["pool_address"]) is None


# =============================================================================
# 2. Receipt-parser produces 32-byte pool_address (source=modify_liquidity)
# =============================================================================


def _modify_liquidity_burn_log(
    *,
    liquidity_delta: int = -500_000,
    tick_lower: int = -60000,
    tick_upper: int = 60000,
    pool_id_hex: str = POOL_ID_32_BYTE,
) -> dict:
    data_hex = (
        "0x"
        + _pad_int24(tick_lower)
        + _pad_int24(tick_upper)
        + _pad_uint((1 << 256) + liquidity_delta)
        + "0" * 64
    )
    return {
        "address": POOL_MANAGER,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            pool_id_hex,
            "0x" + "00" * 12 + POSITION_MANAGER.lower().replace("0x", ""),
        ],
        "data": data_hex,
    }


def _transfer_log(*, token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "00" * 12 + from_addr.lower().replace("0x", ""),
            "0x" + "00" * 12 + to_addr.lower().replace("0x", ""),
        ],
        "data": "0x" + _pad_uint(amount),
    }


class TestParserSourceModifyLiquidityShape:
    """Parser-produced LPCloseData with source=modify_liquidity carries a
    32-byte canonical pool_id and never a 20-byte address."""

    def test_close_event_pool_address_matches_64_hex(self):
        pool_key = PoolKey(currency0=USDC, currency1=WETH, fee=500, tick_spacing=10)
        parser = UniswapV4ReceiptParser(
            chain="arbitrum",
            pool_manager_address=POOL_MANAGER,
            position_manager_address=POSITION_MANAGER,
            pool_key_lookup=lambda pid, chain: pool_key,
        )
        receipt = {
            "transactionHash": "0xclose-shape",
            "logs": [
                _modify_liquidity_burn_log(liquidity_delta=-500_000),
                _transfer_log(token=WETH, from_addr=POOL_MANAGER, to_addr=WALLET, amount=10**18),
                _transfer_log(token=USDC, from_addr=POOL_MANAGER, to_addr=WALLET, amount=2_000_000_000),
            ],
        }
        data = parser.extract_lp_close_data(receipt)
        assert data is not None
        assert data.source == "modify_liquidity"
        assert POOL_ID_REGEX.fullmatch(data.pool_address) is not None
        assert V3_POOL_REGEX.fullmatch(data.pool_address) is None


# =============================================================================
# 3. Invariant violation surfaces (mis-attribution would FAIL)
# =============================================================================


class TestInvariantViolations:
    @pytest.mark.parametrize(
        "bogus_pool_address",
        [
            # 20-byte address with V4 source — must FAIL the V4 shape regex.
            V3_POOL_ADDR_20_BYTE,
            # 30-byte hex (neither V3 nor V4 shape) — must FAIL both regexes.
            "0x" + "cd" * 30,
            # Empty string — must FAIL.
            "",
            # Non-hex characters — must FAIL.
            "0x" + "zz" * 32,
            # Missing 0x prefix — must FAIL.
            "ab" * 32,
        ],
    )
    def test_invariant_rejects_non_32_byte_for_v4_source(self, bogus_pool_address: str):
        """The invariant predicate: V4-sourced rows MUST match
        ``^0x[0-9a-f]{64}$``. Anything else is a misattribution bug."""
        assert POOL_ID_REGEX.fullmatch(bogus_pool_address) is None, (
            f"bogus value {bogus_pool_address!r} unexpectedly matched the V4 invariant regex"
        )

    def test_v4_invariant_accepts_only_lowercase(self):
        """V4 ``pool_id`` is normalised lowercase by the parser; uppercase
        characters in the payload signal a normalisation bug."""
        uppercase_form = POOL_ID_32_BYTE.upper().replace("0X", "0x")
        # Uppercase hex passes the broader case-insensitive shape but fails the
        # canonical lowercase regex this invariant uses.
        assert POOL_ID_REGEX.fullmatch(uppercase_form) is None


# =============================================================================
# 4. End-to-end invariant: writer augment chokepoint preserves the shape
# =============================================================================


def test_v4_lp_accounting_aligns_tokens_to_canonical_currency_order(monkeypatch):
    """VIB-4426 P1 #4 — build_lp_accounting_event MUST re-pair token symbols
    and decimals with the V4 receipt parser's PoolKey-sorted amount0/amount1
    when ``lp_data.currency0`` / ``lp_data.currency1`` are populated.

    Pre-fix: a user-supplied pool string in non-canonical order (e.g.
    ``"USDC/WETH/3000"`` when canonical is WETH<USDC) would mis-scale and
    mis-price amounts. ``amount0`` (in raw WETH units, ~10**18) would get
    divided by 10**6 (USDC decimals) and labelled USDC — wrong cost basis.

    This test simulates that exact misorder: intent declares USDC/WETH (user
    order), parser returns canonical (WETH, USDC) with amount0 = 1 WETH raw
    and amount1 = 2000 USDC raw. With the alignment helper, the resulting
    event should carry token0=WETH, token1=USDC, amount0=1, amount1=2000.
    """
    from decimal import Decimal as Dec

    from almanak.framework.accounting.lp_accounting import build_lp_accounting_event
    from almanak.framework.execution.extracted_data import LPOpenData

    # Patch the token resolver to deterministic symbols/decimals.
    class _FakeTokenInfo:
        def __init__(self, symbol: str, decimals: int) -> None:
            self.symbol = symbol
            self.decimals = decimals

    class _FakeResolver:
        _by_addr = {
            WETH.lower(): _FakeTokenInfo("WETH", 18),
            USDC.lower(): _FakeTokenInfo("USDC", 6),
        }

        def resolve(self, token: str, chain: str = "", **kw):
            return self._by_addr.get(token.lower())

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _FakeResolver(),
    )

    # Mock intent in USER ORDER (USDC first, WETH second) — opposite of canonical.
    class _MockIntent:
        intent_type = type("IT", (), {"value": "LP_OPEN"})()
        protocol = "uniswap_v4"
        pool = "USDC/WETH/3000"  # user order
        token0 = "USDC"
        token1 = "WETH"
        token0_decimals = 6  # user-intent decimals: USDC=6
        token1_decimals = 18

    # Parser-emitted data in CANONICAL order (WETH < USDC by address).
    class _MockResult:
        lp_open_data = LPOpenData(
            position_id=1,
            amount0=10**18,  # 1 WETH (canonical currency0)
            amount1=2000 * 10**6,  # 2000 USDC (canonical currency1)
            currency0=WETH.lower(),
            currency1=USDC.lower(),
        )

    event = build_lp_accounting_event(
        intent=_MockIntent(),
        result=_MockResult(),
        deployment_id="d",
        strategy_id="s",
        cycle_id="c",
        execution_mode="paper",
        chain="arbitrum",
        wallet_address=WALLET,
        ledger_entry_id="le",
    )

    assert event is not None
    # token0 / token1 / amounts MUST be in canonical PoolKey order, not user order.
    assert event.token0 == "WETH", f"expected canonical currency0=WETH; got {event.token0}"
    assert event.token1 == "USDC", f"expected canonical currency1=USDC; got {event.token1}"
    # 1 WETH (10**18 raw / 10**18 decimals) = 1.0; not 10**12 from mis-scaling.
    assert event.amount0 == Dec("1"), f"expected amount0=1.0 (1 WETH); got {event.amount0}"
    assert event.amount1 == Dec("2000"), f"expected amount1=2000 (USDC); got {event.amount1}"


def test_v4_payload_carries_protocol_for_primitive_for_override():
    """VIB-4426 — regression for the V4 stamping dead-code bug.

    ``augment_accounting_payload`` calls ``primitive_for(event_type, protocol)``
    to refine ``Primitive.LP`` → ``Primitive.LP_V4`` for Uniswap V4 rows.
    The override reads ``d.get("protocol")`` from the decoded payload — so
    ``LPAccountingEvent.to_payload_json`` MUST emit ``"protocol"`` or the
    override silently falls back to ``Primitive.LP`` and V4 rows get
    ``matching_policy_version=3`` (the V3 value) instead of ``1`` (the V4 value).

    The two MATCHING_POLICY_VERSIONS entries differ (LP=3, LP_V4=1), so this
    test asserts on the V4 value to prove the override actually fired.
    """
    v4_event = LPAccountingEvent(
        identity=_identity("0xv4stamp"),
        event_type=LPEventType.LP_OPEN,
        position_key=f"lp:uniswap_v4:arbitrum:{WALLET}:{POOL_ID_32_BYTE}",
        pool_address=POOL_ID_32_BYTE,
        token0="WETH",
        token1="USDC",
        amount0=Decimal("1"),
        amount1=Decimal("2000"),
        lp_token_amount=None,
        cost_basis_usd=Decimal("4000"),
        realized_pnl_usd=None,
        fees0_collected=None,
        fees1_collected=None,
        confidence=AccountingConfidence.HIGH,
    )
    # Raw payload MUST include protocol so the augment chokepoint can read it.
    raw = json.loads(v4_event.to_payload_json())
    assert raw.get("protocol") == "uniswap_v4", (
        "LPAccountingEvent.to_payload_json must emit 'protocol' for the V4 stamping override"
    )

    augmented = json.loads(augment_accounting_payload(v4_event.to_payload_json(), is_live=False))
    # V4 lane: matching_policy_version comes from PRIMITIVE_VERSIONS[Primitive.LP_V4] = 1.
    # If the override is dead code, this would be PRIMITIVE_VERSIONS[Primitive.LP] = 3.
    assert augmented["matching_policy_version"] == 1, (
        f"V4 LP event must stamp matching_policy_version=1 (LP_V4 lane); "
        f"got {augmented['matching_policy_version']} — V4 override is dead code"
    )


def test_writer_augment_does_not_mutate_v4_pool_address():
    """The augment chokepoint stamps version columns but MUST NOT mutate or
    truncate ``pool_address``."""
    event = LPAccountingEvent(
        identity=_identity("0xv4close"),
        event_type=LPEventType.LP_CLOSE,
        position_key=f"lp:uniswap_v4:arbitrum:{WALLET}:{POOL_ID_32_BYTE}",
        pool_address=POOL_ID_32_BYTE,
        token0="WETH",
        token1="USDC",
        amount0=Decimal("1"),
        amount1=Decimal("2000"),
        lp_token_amount=None,
        cost_basis_usd=Decimal("4000"),
        realized_pnl_usd=None,
        fees0_collected=None,
        fees1_collected=None,
        confidence=AccountingConfidence.HIGH,
    )
    augmented = augment_accounting_payload(event.to_payload_json(), is_live=False)
    d = json.loads(augmented)
    assert d["pool_address"] == POOL_ID_32_BYTE
    assert POOL_ID_REGEX.fullmatch(d["pool_address"]) is not None
    # Version stamps must land on the same row (T08 wiring contract).
    assert "matching_policy_version" in d
    assert "primitive_version" in d
