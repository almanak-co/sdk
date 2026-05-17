"""VIB-4473 — coverage for ``LPOpenData.position_hash`` field plumbing.

The V4 LP accounting epic (VIB-4426) introduces a new lot-matching anchor
on V4 positions: ``position_hash`` = keccak of
``owner ‖ tickLower ‖ tickUpper ‖ salt`` per V4's
``Position.calculatePositionKey``. V3 callers leave it ``None`` and keep
lot-matching on ``position_token_id``.

These tests pin the contract at the foundation layer:

* The dataclass default is ``None`` (V3 callers are unchanged).
* ``LPOpenData(..., position_hash="0x...")`` round-trips through the
  attribute.
* ``to_dict()`` always emits the ``position_hash`` key — even when
  ``None`` — for downstream JSON stability (consumers expect the key
  shape to be stable across protocols).
* The V3 receipt parser construction path produces ``position_hash=None``.
* ``build_lp_accounting_event`` reads ``lp_open_data.position_hash`` and
  threads it into both the ``LPAccountingEvent`` attribute and the
  ``to_payload_json`` JSON, with V3 (None) and V4 (string) both
  preserved bit-correctly.

T05 (next ticket) wires the actual hash computation into the V4 receipt
parser; nothing here exercises real hash bytes.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.execution.extracted_data import LPOpenData


# ---------------------------------------------------------------------------
# LPOpenData dataclass contract
# ---------------------------------------------------------------------------


class TestLPOpenDataPositionHashField:
    def test_default_is_none(self) -> None:
        """Default value for V3 callers — no explicit argument."""
        lp = LPOpenData(position_id=42)
        assert lp.position_hash is None

    def test_explicit_v3_none_accepted(self) -> None:
        """V3 receipt parser passes ``position_hash=None`` explicitly."""
        lp = LPOpenData(position_id=42, position_hash=None)
        assert lp.position_hash is None

    def test_explicit_v4_string_round_trips(self) -> None:
        """V4 will populate a 32-byte hex hash."""
        h = "0x" + "ab" * 32
        lp = LPOpenData(position_id=42, position_hash=h)
        assert lp.position_hash == h

    def test_to_dict_emits_key_when_none(self) -> None:
        """JSON-stability contract — key MUST be present even when value is None."""
        lp = LPOpenData(position_id=42)
        d = lp.to_dict()
        assert "position_hash" in d, "to_dict() must always emit the key"
        assert d["position_hash"] is None

    def test_to_dict_emits_string_when_populated(self) -> None:
        h = "0x" + "cd" * 32
        lp = LPOpenData(position_id=42, position_hash=h)
        d = lp.to_dict()
        assert d["position_hash"] == h

    def test_to_dict_includes_pre_existing_keys(self) -> None:
        """Adding ``position_hash`` MUST NOT drop V3 fields."""
        lp = LPOpenData(
            position_id=42,
            tick_lower=-100,
            tick_upper=100,
            liquidity=10**18,
            amount0=10**6,
            amount1=10**18,
            current_tick=0,
            pool_address="0xpool",
        )
        d = lp.to_dict()
        for key in (
            "position_id",
            "tick_lower",
            "tick_upper",
            "liquidity",
            "amount0",
            "amount1",
            "current_tick",
            "pool_address",
            "position_hash",
        ):
            assert key in d, f"to_dict() missing key {key!r}"

    def test_frozen(self) -> None:
        """Dataclass is frozen — position_hash cannot be mutated post-construction."""
        from dataclasses import FrozenInstanceError

        lp = LPOpenData(position_id=42)
        try:
            lp.position_hash = "0xdead"  # type: ignore[misc]
        except FrozenInstanceError:
            return
        raise AssertionError("expected FrozenInstanceError when mutating position_hash")


# ---------------------------------------------------------------------------
# V3 receipt parser: position_hash stays None
# ---------------------------------------------------------------------------


class TestV3ReceiptParserKeepsPositionHashNone:
    def test_v3_receipt_parser_returns_none_position_hash(self) -> None:
        """V3 ``extract_lp_open_data`` constructs LPOpenData with position_hash=None.

        We don't run the full parser (it requires a captured receipt); instead
        we assert the explicit kwarg is part of the V3 construction call so the
        defensive guarantee in scope can't silently drift. The kwarg is checked
        as source text — a refactor that drops it would have to update this
        test (and revisit the V3-vs-V4 split before doing so).
        """
        import pathlib

        path = pathlib.Path(
            __file__
        ).parents[3] / "almanak" / "framework" / "connectors" / "uniswap_v3" / "receipt_parser.py"
        src = path.read_text(encoding="utf-8")
        assert "position_hash=None" in src, (
            "V3 receipt_parser.py must construct LPOpenData with explicit "
            "position_hash=None (VIB-4473). If you removed the explicit kwarg, "
            "the dataclass default still keeps V3 None, but the defensive "
            "anchor was load-bearing for future audits."
        )


# ---------------------------------------------------------------------------
# lp_accounting.py threads position_hash through
# ---------------------------------------------------------------------------


def _make_lp_intent(intent_type_str: str, *, protocol: str = "uniswap_v3") -> MagicMock:
    intent = MagicMock()
    it = MagicMock()
    it.value = intent_type_str
    intent.intent_type = it
    intent.protocol = protocol
    intent.pool = "USDC/WETH/0xpool"
    intent.token0 = "USDC"
    intent.token1 = "WETH"
    intent.token0_decimals = 6
    intent.token1_decimals = 18
    return intent


def _make_result_with_lp_open(lp_open_data: LPOpenData | None) -> MagicMock:
    result = MagicMock()
    result.tx_hash = "0xtxhash"
    result.transaction_results = []
    result.lp_open_data = lp_open_data
    result.lp_close_data = None
    result.extracted_data = {}
    return result


_COMMON_KWARGS = dict(
    deployment_id="deploy-1",
    strategy_id="strat-1",
    cycle_id="cycle-1",
    execution_mode="paper",
    chain="base",
    wallet_address="0xwallet",
)


class TestLPAccountingThreadsPositionHash:
    def test_v3_open_event_has_none_position_hash(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = LPOpenData(
            position_id=42,
            amount0=100_000_000,
            amount1=10**18,
            # position_hash omitted — defaults to None (V3 contract).
        )
        intent = _make_lp_intent("LP_OPEN", protocol="uniswap_v3")
        result_obj = _make_result_with_lp_open(lp_open)

        event = build_lp_accounting_event(intent=intent, result=result_obj, **_COMMON_KWARGS)

        assert event is not None
        assert event.position_hash is None

    def test_v3_payload_json_emits_none_position_hash_key(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = LPOpenData(position_id=42, amount0=100_000_000, amount1=10**18)
        intent = _make_lp_intent("LP_OPEN", protocol="uniswap_v3")
        result_obj = _make_result_with_lp_open(lp_open)

        event = build_lp_accounting_event(intent=intent, result=result_obj, **_COMMON_KWARGS)
        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert "position_hash" in payload
        assert payload["position_hash"] is None

    def test_v4_open_event_has_populated_position_hash(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        hash_hex = "0x" + "ef" * 32
        lp_open = LPOpenData(
            position_id=42,
            amount0=100_000_000,
            amount1=10**18,
            position_hash=hash_hex,
        )
        intent = _make_lp_intent("LP_OPEN", protocol="uniswap_v4")
        result_obj = _make_result_with_lp_open(lp_open)

        event = build_lp_accounting_event(intent=intent, result=result_obj, **_COMMON_KWARGS)

        assert event is not None
        assert event.position_hash == hash_hex
        payload = json.loads(event.to_payload_json())
        assert payload["position_hash"] == hash_hex

    def test_lp_close_emits_none_position_hash(self) -> None:
        """LP_CLOSE leaves position_hash None: close-side matches by position_key.

        The close leg uses the prior LP_OPEN payload as its lot-matching
        partner. position_hash is the OPEN-side anchor only.
        """
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        result = MagicMock()
        result.tx_hash = "0xtxhash"
        result.transaction_results = []
        result.lp_open_data = None
        lp_close = MagicMock()
        lp_close.amount0_collected = 95_000_000
        lp_close.amount1_collected = 95 * 10**18
        lp_close.fees0 = 500_000
        lp_close.fees1 = 5 * 10**17
        result.lp_close_data = lp_close
        result.extracted_data = {}

        intent = _make_lp_intent("LP_CLOSE", protocol="uniswap_v3")
        event = build_lp_accounting_event(intent=intent, result=result, **_COMMON_KWARGS)

        assert event is not None
        assert event.position_hash is None
        payload = json.loads(event.to_payload_json())
        assert "position_hash" in payload
        assert payload["position_hash"] is None


# ---------------------------------------------------------------------------
# Cost-basis Decimal sanity (guard that the new field didn't perturb pricing)
# ---------------------------------------------------------------------------


class TestPriceOracleStillWorks:
    """Smoke check — position_hash threading didn't perturb cost_basis_usd."""

    def test_cost_basis_unchanged_when_position_hash_set(self) -> None:
        from almanak.framework.accounting.lp_accounting import build_lp_accounting_event

        lp_open = LPOpenData(
            position_id=42,
            amount0=100_000_000,  # 100 USDC
            amount1=10**18,  # 1 WETH
            position_hash="0x" + "ab" * 32,
        )
        intent = _make_lp_intent("LP_OPEN", protocol="uniswap_v4")
        result_obj = _make_result_with_lp_open(lp_open)

        price_oracle = {"USDC": Decimal("1.00"), "WETH": Decimal("2000")}

        event = build_lp_accounting_event(
            intent=intent,
            result=result_obj,
            price_oracle=price_oracle,
            **_COMMON_KWARGS,
        )
        assert event is not None
        # 100 USDC + 1 WETH * 2000 = 2100
        assert event.cost_basis_usd == Decimal("2100.00")
        assert event.position_hash == "0x" + "ab" * 32
